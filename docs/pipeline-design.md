# Ordered Processing Pipeline Design

[//]: # (TODO: add the link to the Beam Javadoc)
The mechanics of the ordering processing are handled by a generic [OrderedEventProcessor]()
transform from Apache Beam's "ordered" extension module.
It uses Apache Beam's state and timers to keep the state of processing, buffers the events that
arrived out-of-sequence and emits the processing status events.

The following are a few simple steps required to make the OrderedEventProcessor to work for a
particular use case.

## Transform the data into the shape needed by the OrderedEventProcessor

### Create the key class (if needed)

Ordered processing is done for a particular "key". Typically, there are many keys in the incoming
data. In our use case the key is the combination of session id and contract
id. [SessionContractKey](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/SessionContractKey.java)
is a class which implements both the key and its coder.

### Transform the incoming PCollection into a PCollection of the required shape

The input data for the OrderedEventProcessor needs to be a `KV<KeyType, <KV<Long,EventType>>`
PCollection.
The key type in our case is the SessionContractKey, event type - OrderBookEvent.
[ConvertOrderBookEventToKV DoFn](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/ConvertOrderBookEventToKV.java)
puts our incoming PCollection into the required shape.

### Create a class to wrap your business logic of processing events

[//]: # (TODO: add the link to the Beam Javadoc)
This class needs to implement [MutableState interface](), and must implement these two methods:

* `mutate` will be called by the OrderedEventProcessor in the right sequence
* `produceResult` will follow the call to `mutate` or the initial creation of the state. It can
  return "null" if there is nothing to output, or produce the result, which will be immediately
  output to the resulting PCollection.

It is implemented
in [OrderBookMutableState class](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookMutableState.java).

### Create a class which will be used to analyse events

Besides knowing the sequence number of the event in the context of a particular key, the ordered
processor also needs to know several additional things about the current element. [//]: # (TODO: add
the link to the Beam Javadoc)
This information is provided by a class which implements [EventExaminer interface](). In our demo it
is [OrderBookEventExaminer class](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookEventExaminer.java).

It has the following methods:

* `isInitialEvent` - Is it the first element in the sequence? It doesn't assume that 0 or 1 is the
  starting sequence
  number.
* `createStateOnInitialEvent` - How to create the state when the initial event arrived? Intial state
  creation and be simple, or
  can involve complex logic, including making external API calls.
* `isLastEvent` - Is it the last element in the sequence? This information is used to do some
  cleanup if all the
  expected elements for a given key have been processed.

### Create coders

Coders are needed by the OrderedProcessor transform to serialize and deserialize the data until it's
ready to be processed. There are three coders used by the transform:

* Mutable state
  coder - [OrderBookCoder](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookCoder.java)
* Event coder to be used to store the buffered (out-of-sequence)
  events - [ProtoCoder in OrderBookProducer](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookProducer.java)
* Key
  coder - [SessionContractKeyCoder in SessionContractKey class](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/SessionContractKey.java).

### Create a handler to tie all the above together

[//]: # (TODO: add the link to the Beam Javadoc)
The OrderedEventProcessor is configured by providing a handler which must extend
the [OrderedProcessingHandler]()
class. That class (in this
demo, [OrderBookOrderedProcessingHandler](../order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookOrderedProcessingHandler.java))
provides default implementations for a number of methods. The implementing class
must provide two methods:

* a constructor which provides the parent class' constructor with the classes of the event, key,
  state, and the result. The parent class will use this information to attempt to determine the
  coders for these classes.
* `getEventExaminer` method to return a class implementing the `EventExaminer` interface.

You can override the default implementations of the methods to supply custom coders (different from
the default registered with the pipeline), change the processing status notification emission
frequency, buffering parameters, etc.

[//]: # (TODO: add the link to the Beam Javadoc)
`OrderedEventProcessor` can process two kinds of sequences - a sequence per key and a global
sequence. It determines which sequencing type is used based on which class the provided handlers
extends. If OrderedProcessingHandler is extended then a sequence per key will be used.
If OrderedProcessingHandler.OrderedProcessingGlobalSequenceHandler is extended - the global sequence
will be utilized.

### Create a custom transform to wrap the OrderedEventProcessing transform

This is an optional step, and technically you don't have to do it. But if you do - the main pipeline
code will look more compact and the graph on the Dataflow UI will look "prettier". In our
demo [OrderBookProducer transform](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookProducer.java)
is used to hide some mechanics of using the OrderedEventProcessor.

### Decide where you would like to store the results of the processing

Our demo uses BigQuery tables to store the market depths produced by the order book builder,
processing statuses output by the OrderedEventProcessor and the source order events. You
would need to code classes that transform these classes to TableRows. An example of these class
is [MarketDepthToTableRowConverter](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/MarketDepthToTableRowConverter.java).

### Code the pipeline

The final pipeline is very simple at this point: read the sources (Google Pub/Sub subscription in
our case), build the order book and save the
output - [OrderBookProcessingPipeline](/order-book-pipeline/src/main/java/com/google/cloud/dataflow/orderbook/OrderBookProcessingPipeline.java).

This is the pipeline graph:

![pipeline graph](pipeline-graph.png)

## Fine print

### Duplicate/unprocessed events

The OrderedProcessor transform will output the events which have the order number lower than the
currently processed order number or could not be processed due to an exception being thrown in
a dedicated PCollection - OrderedEventProcessorResult's
`unprocessedEvents`. The demo pipeline doesn't process this PCollection, but production pipelines
will most likely will need to process this PCollection.

The number of detected duplicates will be reported in the emitted processing statuses.

[//]: # (TODO: check why not implemented)

### Draining a streaming Dataflow pipeline

In case a streaming pipeline which runs using the Dataflow runner is drained and still has a number
of unprocessed (buffered) events, these events will be output via the unprocessedEvents PCollection
with `buffered` reason.