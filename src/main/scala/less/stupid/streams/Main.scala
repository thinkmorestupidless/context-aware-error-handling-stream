package less.stupid.streams

import akka.{Done, NotUsed}
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Flow, FlowWithContext, Keep, RestartFlow, RunnableGraph, Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import less.stupid.streams.aws.domain.{KinesisContext, KinesisProcessingFlow}
import less.stupid.streams.aws.infrastructure.{KinesisStream, SuccessfulOffsetWriter}
import less.stupid.streams.domain.Decoder.{DecodingError, UnableToDecodeBecauseItsHardcodedToFail}
import less.stupid.streams.domain.Service.{ServiceCallFailedBecauseSomeWeirdThingHappened, ServiceError}
import less.stupid.streams.domain._
import less.stupid.streams.infrastructure.{SometimesFailingService, SuccessfulDecoder}
import less.stupid.streams.messaging.domain.OffsetWriter
import less.stupid.streams.messaging.domain.OffsetWriter.{OffsetWriterError, OffsetWriterException}
import less.stupid.streams.messaging.domain.OffsetWritingMessageStream.{OffsetWritingFlow, OffsetWritingProcessingFlow}
import less.stupid.streams.messaging.infrastructure.MessageFlow.{MessageFlow, mapAsyncErrorOr}
import less.stupid.streams.messaging.infrastructure.OffsetWritingStream
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

object Main {

  private val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

//    val decoder = new SometimesFailingDecoder
    val service = new SometimesFailingService

    val decoder = new SuccessfulDecoder
//    val service = new SuccessfulService

    val offsetWriter = new SuccessfulOffsetWriter

    implicit val system = ActorSystem(Behaviors.empty, "TheSystem")
    implicit val ec: ExecutionContext = system.executionContext

    val restartSettings = RestartSettings(
      minBackoff = 3.seconds,
      maxBackoff = 30.seconds,
      randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
    ).withMaxRestarts(20, 5.minutes) // limits the amount of restarts to 20 within 5 minutes

    val source = Source((1 to 10).toList.map(int => EncodedMessage(int.toString)))

    val processingFlow: KinesisProcessingFlow[DomainError, EncodedMessage] =
      FlowWithContext[EncodedMessage, KinesisContext]
        .mapAsync(1)(decoder.decode)
        .via(mapAsyncErrorOr(1)(decodedMessage => service.send(ServiceRequest(decodedMessage))))

    // take a Source[EncodedMessage, _]
    // create a context for the message (here, we're just wrapping it in `KinesisContext`)
    // run it through the processing steps above
    // write the offset
    // ... restart settings for the offset writing - not required for the PoC, but useful for thinking
    // about how to pass through these kinds of settings...
    val graph = KinesisStream(source, KinesisContext, processingFlow, offsetWriter, restartSettings)

    graph.run().onComplete {
      case Success(_) => System.exit(0)
      case Failure(e) => {
        log.info(s"Stream failed [reason=$e]")
        System.exit(1)
      }
    }
  }
}

object domain {

  trait DomainError

  case class EncodedMessage(value: String)
  case class DecodedMessage(value: String)

  trait Decoder {
    def decode(from: EncodedMessage): Future[Either[DecodingError, DecodedMessage]]
  }

  object Decoder {
    sealed trait DecodingError extends DomainError
    case object UnableToDecodeBecauseItsHardcodedToFail extends DecodingError
  }

  case class ServiceRequest(decodedMessage: DecodedMessage)
  case class ServiceResponse(request: ServiceRequest)

  trait Service {
    def send(request: ServiceRequest): Future[Either[ServiceError, ServiceResponse]]
  }

  object Service {
    sealed trait ServiceError extends DomainError
    case object ServiceCallFailedBecauseSomeWeirdThingHappened extends ServiceError
  }
}

object infrastructure {

  class SuccessfulDecoder extends Decoder {
    override def decode(from: EncodedMessage): Future[Either[DecodingError, DecodedMessage]] =
      Future.successful(Right(DecodedMessage(from.value)))
  }

  class SometimesFailingDecoder extends Decoder {

    private val log = LoggerFactory.getLogger(getClass)

    override def decode(from: EncodedMessage): Future[Either[DecodingError, DecodedMessage]] = {
      if (Random.nextFloat() > 0.85) {
        log.info(s"decoding failed for $from")
        Future.successful(Left(UnableToDecodeBecauseItsHardcodedToFail))
      } else {
        Future.successful(Right(DecodedMessage(from.value)))
      }
    }
  }

  class SuccessfulService extends Service {
    override def send(request: ServiceRequest): Future[Either[ServiceError, ServiceResponse]] =
      Future.successful(Right(ServiceResponse(request)))
  }

  class SometimesFailingService extends Service {

    private val log = LoggerFactory.getLogger(getClass)

    override def send(request: ServiceRequest): Future[Either[ServiceError, ServiceResponse]] =
      if (Random.nextFloat() > 0.85) {
        log.info(s"service call failed for $request")
        Future.successful(Left(ServiceCallFailedBecauseSomeWeirdThingHappened))
      } else {
        Future.successful(Right(ServiceResponse(request)))
      }
  }
}

object aws {

  object domain {

    case class KinesisContext(value: EncodedMessage)

    type KinesisOffsetWritingFlow[Error] = OffsetWritingFlow[Error, KinesisContext]
    type KinesisProcessingFlow[Error, In] = OffsetWritingProcessingFlow[Error, KinesisContext, In]
  }

  object infrastructure {

    class SuccessfulOffsetWriter extends OffsetWriter {

      private val log = LoggerFactory.getLogger(getClass)

      override def writeOffsetForContext[Error, Context](
          errorOr: Either[Error, Unit],
          context: Context): Future[Either[OffsetWriterError, Unit]] = {
        errorOr match {
          case Left(error) => log.info(s"Writing offset for failing flow element [error=$error]")
          case Right(_)    => log.info(s"Writing offset for successful flow element")
        }

        Future.successful(Right(()))
      }
    }

    object KinesisStream {

      def apply[Error, Context, In](
          source: Source[In, NotUsed],
          contextProvider: In => KinesisContext,
          processingFlow: KinesisProcessingFlow[Error, In],
          offsetWriter: OffsetWriter,
          restartSettings: RestartSettings)(implicit
          ec: ExecutionContext,
          mat: Materializer): RunnableGraph[Future[Done]] =
        OffsetWritingStream(source, contextProvider, processingFlow, offsetWriter, restartSettings)
    }
  }
}

object messaging {

  object domain {

    object OffsetWritingMessageStream {

      type OffsetWritingFlow[Error, Context] = Flow[(Either[Error, Unit], Context), Unit, NotUsed]
      type OffsetWritingProcessingFlow[Error, Context, In] =
        FlowWithContext[In, Context, Either[Error, _], Context, NotUsed]
    }

    trait OffsetWriter {
      def writeOffsetForContext[Error, Context](
          errorOrMessage: Either[Error, Unit],
          context: Context): Future[Either[OffsetWriterError, Unit]]
    }

    object OffsetWriter {
      sealed trait OffsetWriterError
      class OffsetWriterException(offsetWriterError: OffsetWriterError)
          extends RuntimeException(s"Error writing offset [$offsetWriterError]")
    }
  }

  object infrastructure {

    object MessageFlow {

      type MessageFlow[Error, Context, In, Out] =
        FlowWithContext[Either[Error, In], Context, Either[Error, Out], Context, NotUsed]

      /**
       * Either[Error, IncomingMessage] => Either[Error, OutgoingMessage]
       *
       * If it receives a `Left` it just propagates forward.
       * If it receives a `Right` it invokes `block` and passes the value of `Right` as the argument.
       */
      def mapAsyncErrorOr[Error, Context, In, Out](parallelism: Int)(
          block: In => Future[Either[Error, Out]]): MessageFlow[Error, Context, In, Out] =
        FlowWithContext[Either[Error, In], Context].mapAsync(parallelism) {
          case Left(error) => Future.successful(Left(error))
          case Right(elem) => block(elem)
        }
    }

    object OffsetWritingStream {

      private val log = LoggerFactory.getLogger(getClass)

      def apply[Error, Context, In](
          source: Source[In, NotUsed],
          contextProvider: In => Context,
          processingFlow: OffsetWritingProcessingFlow[Error, Context, In],
          offsetWriter: OffsetWriter,
          restartSettings: RestartSettings)(implicit
          ec: ExecutionContext,
          mat: Materializer): RunnableGraph[Future[Done]] = {
        val offsetWritingFlow = OffsetWritingFlow[Error, Context](offsetWriter, restartSettings)
        source
          .asSourceWithContext(contextProvider)
          .via(processingFlow)
          .asSource
          .via(toUnit)
          .via(offsetWritingFlow)
          .toMat(Sink.foreach(_ => log.info("processing complete")))(Keep.right)
      }

      /**
       * Convenience for mapping anything in a `Right` to a `Right(Unit)`
       * We _might_ need to know whether the processing of the message was successful or not,
       * but we don't need to know anything about the message itself (that would also cause
       * problems with leaking domain knowledge into this AWS package/module.
       */
      def toUnit[Error, Context, In]: MessageFlow[Error, Context, In, Unit] =
        FlowWithContext[Either[Error, _], Context].map(_.map(_ => ()))
    }

    object OffsetWritingFlow {

      def apply[Error, Context](offsetWriter: OffsetWriter, restartSettings: RestartSettings)(implicit
          ec: ExecutionContext): OffsetWritingFlow[Error, Context] =
        RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
          Flow[(Either[Error, Unit], Context)].mapAsync(1) {
            case (errorOr, context) =>
              offsetWriter.writeOffsetForContext(errorOr, context).map {
                case Left(error) => throw new OffsetWriterException(error)
                case Right(_)    => ()
              }
          }
        }
    }
  }
}
