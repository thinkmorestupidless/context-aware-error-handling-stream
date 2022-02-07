# Context-Aware Error Handling Stream

An attempt to define an abstraction for a stream which consumes messages from a broker (requiring it to maintain context along the flow in order to write offsets/acks/nacks at the end of the flow) whilst also handling errors in flow stages.

For instance:

```
Amazon Kinesis Source ~> Decoding Step ~> API call ~> Amazon Kinesis Sink
```

If there's a failure at the `Decoding` step then the `API Call` step can't be attempted, however we will want to write the offset back at the end of the stream despite this failure.

