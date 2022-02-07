package less.stupid.streams

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.stream.scaladsl.{Flow, FlowWithContext, RestartFlow, RunnableGraph, Sink, Source}
import akka.stream.{Materializer, RestartSettings}
import less.stupid.streams.aws.domain.OffsetWriter.{OffsetWriterError, OffsetWriterException}
import less.stupid.streams.aws.domain.{KinesisContext, KinesisOffsetWritingFlow, KinesisProcessingFlow, OffsetWriter}
import less.stupid.streams.aws.infrastructure.{KinesisStream, SuccessfulOffsetWriter}
import less.stupid.streams.domain.Decoder.{DecodingError, UnableToDecodeBecauseItsHardcodedToFail}
import less.stupid.streams.domain.Service.{ServiceCallFailedBecauseSomeWeirdThingHappened, ServiceError}
import less.stupid.streams.domain._
import less.stupid.streams.infrastructure.{SometimesFailingService, SuccessfulDecoder}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object Main {

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
        .mapAsync(1) {
          case Left(error)           => Future.successful(Left(error))
          case Right(decodedMessage) => service.send(ServiceRequest(decodedMessage))
        }
        .map {
          case Right(_) => Right(())
          case Left(e)  => Left(e)
        }

    val graph = KinesisStream(source, KinesisContext, processingFlow, offsetWriter, restartSettings)

    graph.run()
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

    type KinesisOffsetWritingFlow[Error] = Flow[(Either[Error, Unit], KinesisContext), Unit, NotUsed]
    type KinesisProcessingFlow[Error, In] =
      FlowWithContext[In, KinesisContext, Either[Error, Unit], KinesisContext, NotUsed]

    trait OffsetWriter {
      def writeOffsetForContext[Error](
          errorOrMessage: Either[Error, Unit],
          context: KinesisContext): Future[Either[OffsetWriterError, Unit]]
    }

    object OffsetWriter {
      sealed trait OffsetWriterError
      class OffsetWriterException(offsetWriterError: OffsetWriterError)
          extends RuntimeException(s"Error writing offset [$offsetWriterError]")
    }
  }

  object infrastructure {

    class SuccessfulOffsetWriter extends OffsetWriter {

      private val log = LoggerFactory.getLogger(getClass)

      override def writeOffsetForContext[Error](
          errorOr: Either[Error, Unit],
          context: domain.KinesisContext): Future[Either[OffsetWriterError, Unit]] = {
        errorOr match {
          case Left(error) => log.info(s"Writing offset for failing flow element [error=$error]")
          case Right(_)    => log.info(s"Writing offset for successful flow element")
        }

        Future.successful(Right(()))
      }
    }

    object KinesisOffsetWritingFlow {

      def apply[Error](offsetWriter: OffsetWriter, restartSettings: RestartSettings)(implicit
          ec: ExecutionContext): KinesisOffsetWritingFlow[Error] =
        RestartFlow.onFailuresWithBackoff(restartSettings) { () =>
          Flow[(Either[Error, Unit], KinesisContext)].mapAsync(1) {
            case (errorOrMessage, context) =>
              offsetWriter.writeOffsetForContext(errorOrMessage, context).map {
                case Left(error) => throw new OffsetWriterException(error)
                case Right(_)    => ()
              }
          }
        }
    }

    object KinesisStream {

      private val log = LoggerFactory.getLogger(getClass)

      def apply[Error, In](
          source: Source[In, NotUsed],
          contextProvider: In => KinesisContext,
          processingFlow: KinesisProcessingFlow[Error, In],
          offsetWriter: OffsetWriter,
          restartSettings: RestartSettings)(implicit
          ec: ExecutionContext,
          mat: Materializer): RunnableGraph[NotUsed] = {
        val offsetWritingFlow = KinesisOffsetWritingFlow[Error](offsetWriter, restartSettings)
        source
          .asSourceWithContext(contextProvider)
          .via(processingFlow)
          .asSource
          .via(offsetWritingFlow)
          .to(Sink.foreach(_ => log.info("processing complete")))
      }
    }
  }
}
