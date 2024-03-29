/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.ordered;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.ProcessingState.ProcessingStateCoder;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.Reason;
import org.apache.beam.sdk.extensions.ordered.UnprocessedEvent.UnprocessedEventCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Transform for processing ordered events. Events are grouped by the key and within each key they
 * are applied according to the provided sequence. Events which arrive out of sequence are buffered
 * and reprocessed when new events for a given key arrived.
 *
 * @param <Event>
 * @param <EventKey>
 * @param <State>
 */
@AutoValue
@SuppressWarnings({"nullness", "TypeNameShadowing"})
public abstract class OrderedEventProcessor<Event, EventKey, Result, State extends MutableState<Event, Result>> extends
    PTransform<PCollection<KV<EventKey, KV<Long, Event>>>, OrderedEventProcessorResult<EventKey, Result, Event>> {


  public static <Event, EventKey, Result, State extends MutableState<Event, Result>> OrderedEventProcessor<Event, EventKey, Result, State> create(
      OrderedProcessingHandler<Event, EventKey, State, Result> handler) {
    return new AutoValue_OrderedEventProcessor<>(handler);
  }

  @Nullable
  abstract OrderedProcessingHandler<Event, EventKey, State, Result> getHandler();

  @Override
  public OrderedEventProcessorResult<EventKey, Result, Event> expand(
      PCollection<KV<EventKey, KV<Long, Event>>> input) {
    final TupleTag<KV<EventKey, Result>> mainOutput = new TupleTag<>("mainOutput") {
    };
    final TupleTag<KV<EventKey, OrderedProcessingStatus>> statusOutput = new TupleTag<>("status") {
    };

    final TupleTag<KV<EventKey, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventOutput = new TupleTag<>(
        "unprocessed-events") {
    };

    OrderedProcessingHandler<Event, EventKey, State, Result> handler = getHandler();
    Pipeline pipeline = input.getPipeline();

    Coder<EventKey> keyCoder;
    try {
      keyCoder = handler.getKeyCoder(pipeline, input.getCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get key coder", e);
    }

    Coder<Event> eventCoder;
    try {
      eventCoder = handler.getEventCoder(pipeline, input.getCoder());
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get event coder", e);
    }

    Coder<State> stateCoder;
    try {
      stateCoder = handler.getStateCoder(pipeline);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get state coder", e);
    }

    Coder<Result> resultCoder;
    try {
      resultCoder = handler.getResultCoder(pipeline);
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Unable to get result coder", e);
    }

    PCollectionTuple processingResult = input.apply(ParDo.of(
        new OrderedProcessorDoFn<>(handler.getEventExaminer(), eventCoder,
            stateCoder,
            keyCoder, mainOutput, statusOutput,
            handler.getStatusUpdateFrequency(),
            unprocessedEventOutput,
            handler.isProduceStatusUpdateOnEveryEvent(),
            input.isBounded() == IsBounded.BOUNDED ? Integer.MAX_VALUE
                : handler.getMaxOutputElementsPerBundle())).withOutputTags(mainOutput,
        TupleTagList.of(Arrays.asList(statusOutput, unprocessedEventOutput))));

    KvCoder<EventKey, Result> mainOutputCoder = KvCoder.of(keyCoder, resultCoder);
    KvCoder<EventKey, OrderedProcessingStatus> processingStatusCoder = KvCoder.of(keyCoder,
        getOrderedProcessingStatusCoder(pipeline));
    KvCoder<EventKey, KV<Long, UnprocessedEvent<Event>>> unprocessedEventsCoder = KvCoder.of(
        keyCoder,
        KvCoder.of(VarLongCoder.of(), new UnprocessedEventCoder<>(eventCoder)));
    return new OrderedEventProcessorResult<>(
        pipeline,
        processingResult.get(mainOutput).setCoder(mainOutputCoder),
        mainOutput,
        processingResult.get(statusOutput).setCoder(processingStatusCoder),
        statusOutput,
        processingResult.get(unprocessedEventOutput).setCoder(unprocessedEventsCoder),
        unprocessedEventOutput);
  }

  private static Coder<OrderedProcessingStatus> getOrderedProcessingStatusCoder(Pipeline pipeline) {
    SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
    Coder<OrderedProcessingStatus> result;
    try {
      result = SchemaCoder.of(schemaRegistry.getSchema(OrderedProcessingStatus.class),
          TypeDescriptor.of(OrderedProcessingStatus.class),
          schemaRegistry.getToRowFunction(OrderedProcessingStatus.class),
          schemaRegistry.getFromRowFunction(OrderedProcessingStatus.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Main DoFn for processing ordered events
   *
   * @param <Event>
   * @param <EventKey>
   * @param <State>
   */
  static class OrderedProcessorDoFn<Event, EventKey, Result, State extends MutableState<Event, Result>> extends
      DoFn<KV<EventKey, KV<Long, Event>>, KV<EventKey, Result>> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedProcessorDoFn.class);

    private static final String PROCESSING_STATE = "processingState";
    private static final String MUTABLE_STATE = "mutableState";
    private static final String BUFFERED_EVENTS = "bufferedEvents";
    private static final String STATUS_EMISSION_TIMER = "statusTimer";
    private static final String LARGE_BATCH_EMISSION_TIMER = "largeBatchTimer";
    private static final String WINDOW_CLOSED = "windowClosed";
    private final EventExaminer<Event, State> eventExaminer;

    @StateId(BUFFERED_EVENTS)
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<Event>> bufferedEventsSpec;

    @StateId(PROCESSING_STATE)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<ProcessingState<EventKey>>> processingStateSpec;

    @SuppressWarnings("unused")
    @StateId(MUTABLE_STATE)
    private final StateSpec<ValueState<State>> mutableStateSpec;

    @StateId(WINDOW_CLOSED)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Boolean>> windowClosedSpec;

    @TimerId(STATUS_EMISSION_TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec statusEmissionTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerId(LARGE_BATCH_EMISSION_TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec largeBatchEmissionTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final TupleTag<KV<EventKey, OrderedProcessingStatus>> statusTupleTag;
    private final Duration statusUpdateFrequency;

    private final TupleTag<KV<EventKey, Result>> mainOutputTupleTag;
    private final TupleTag<KV<EventKey, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventsTupleTag;
    private final boolean produceStatusUpdateOnEveryEvent;

    private final long maxNumberOfResultsToProduce;

    private Long numberOfResultsBeforeBundleStart;

    /**
     * Stateful DoFn to do the bulk of processing
     *
     * @param eventExaminer
     * @param eventCoder
     * @param stateCoder
     * @param keyCoder
     * @param mainOutputTupleTag
     * @param statusTupleTag
     * @param statusUpdateFrequency
     * @param unprocessedEventTupleTag
     * @param produceStatusUpdateOnEveryEvent
     * @param maxNumberOfResultsToProduce
     */
    OrderedProcessorDoFn(
        EventExaminer<Event, State> eventExaminer, Coder<Event> eventCoder,
        Coder<State> stateCoder, Coder<EventKey> keyCoder,
        TupleTag<KV<EventKey, Result>> mainOutputTupleTag,
        TupleTag<KV<EventKey, OrderedProcessingStatus>> statusTupleTag,
        Duration statusUpdateFrequency,
        TupleTag<KV<EventKey, KV<Long, UnprocessedEvent<Event>>>> unprocessedEventTupleTag,
        boolean produceStatusUpdateOnEveryEvent,
        long maxNumberOfResultsToProduce) {
      this.eventExaminer = eventExaminer;
      this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
      this.mutableStateSpec = StateSpecs.value(stateCoder);
      this.processingStateSpec = StateSpecs.value(ProcessingStateCoder.of(keyCoder));
      this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
      this.mainOutputTupleTag = mainOutputTupleTag;
      this.statusTupleTag = statusTupleTag;
      this.unprocessedEventsTupleTag = unprocessedEventTupleTag;
      this.statusUpdateFrequency = statusUpdateFrequency;
      this.produceStatusUpdateOnEveryEvent = produceStatusUpdateOnEveryEvent;
      this.maxNumberOfResultsToProduce = maxNumberOfResultsToProduce;
    }

    @StartBundle
    public void onBundleStart() {
      numberOfResultsBeforeBundleStart = null;
    }

    @FinishBundle
    public void onBundleFinish() {
      // This might be necessary because this field is also used in a Timer
      numberOfResultsBeforeBundleStart = null;
    }

    @ProcessElement
    public void processElement(
        @StateId(BUFFERED_EVENTS) OrderedListState<Event> bufferedEventsState,
        @AlwaysFetched @StateId(PROCESSING_STATE) ValueState<ProcessingState<EventKey>> processingStateState,
        @StateId(MUTABLE_STATE) ValueState<State> mutableStateState,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        @Element KV<EventKey, KV<Long, Event>> eventAndSequence,
        MultiOutputReceiver outputReceiver,
        BoundedWindow window) {

      EventKey key = eventAndSequence.getKey();
      long sequence = eventAndSequence.getValue().getKey();
      Event event = eventAndSequence.getValue().getValue();

      ProcessingState<EventKey> processingState = processingStateState.read();

      if (processingState == null) {
        // This is the first time we see this key/window pair
        processingState = new ProcessingState<>(key);
        if (statusUpdateFrequency != null) {
          // Set up the timer to produce the status of the processing on a regular basis
          statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
        }
      }

      if (numberOfResultsBeforeBundleStart == null) {
        // Per key processing is synchronized by Beam. There is no need to have it here.
        numberOfResultsBeforeBundleStart = processingState.getResultCount();
      }

      processingState.recordReceived();

      State state = processNewEvent(sequence, event, processingState, mutableStateState,
          bufferedEventsState, outputReceiver);

      processBufferedEvents(processingState, state, bufferedEventsState,
          outputReceiver, largeBatchEmissionTimer);

      saveStates(processingStateState, processingState, mutableStateState,
          state, outputReceiver, key, window.maxTimestamp());

      checkIfProcessingIsCompleted(processingState);
    }

    private boolean checkIfProcessingIsCompleted(ProcessingState<EventKey> processingState) {
      boolean result = processingState.isProcessingCompleted();
      if (result) {
        LOG.info("Processing for key '" + processingState.getKey() + "' is completed.");
      }
      return result;
    }

    private void saveStates(
        ValueState<ProcessingState<EventKey>> processingStatusState,
        ProcessingState<EventKey> processingStatus, ValueState<State> currentStateState,
        State state, MultiOutputReceiver outputReceiver, EventKey key, Instant windowTimestamp) {
      // There is always a change to the processing status
      processingStatusState.write(processingStatus);

      // Stored state may not have changes if the element was out of sequence.
      if (state != null) {
        currentStateState.write(state);
      }

      if (produceStatusUpdateOnEveryEvent) {
        // During pipeline draining the window timestamp is set to a large value in the future.
        // Producing an event before that results in error, that's why this logic exist.
        Instant statusTimestamp = Instant.now();
        statusTimestamp =
            statusTimestamp.isAfter(windowTimestamp) ? statusTimestamp : windowTimestamp;

        emitProcessingStatus(processingStatus, outputReceiver, statusTimestamp);
      }
    }

    private void emitProcessingStatus(ProcessingState<EventKey> processingState,
        MultiOutputReceiver outputReceiver, Instant statusTimestamp) {
      outputReceiver.get(statusTupleTag).outputWithTimestamp(KV.of(processingState.getKey(),
          OrderedProcessingStatus.create(processingState.getLastOutputSequence(),
              processingState.getBufferedRecordCount(),
              processingState.getEarliestBufferedSequence(),
              processingState.getLatestBufferedSequence(),
              processingState.getRecordsReceived(),
              processingState.getResultCount(),
              processingState.getDuplicates(),
              processingState.isLastEventReceived())), statusTimestamp);
    }

    /**
     * Process the just received event
     *
     * @param currentSequence
     * @param currentEvent
     * @param processingState
     * @param currentStateState
     * @param bufferedEventsState
     * @param outputReceiver
     * @return
     */
    private State processNewEvent(long currentSequence, Event currentEvent,
        ProcessingState<EventKey> processingState, ValueState<State> currentStateState,
        OrderedListState<Event> bufferedEventsState,
        MultiOutputReceiver outputReceiver) {
      if (currentSequence == Long.MAX_VALUE) {
        LOG.error("Received an event with " + currentSequence + " as the sequence number. "
            + "It will be dropped because it needs to be less than Long.MAX_VALUE.");
        return null;
      }

      if (processingState.hasAlreadyBeenProcessed(currentSequence)) {
        outputReceiver.get(unprocessedEventsTupleTag).output(KV.of(processingState.getKey(),
            KV.of(currentSequence, UnprocessedEvent.create(currentEvent, Reason.duplicate))));
        return null;
      }

      State state;
      boolean thisIsTheLastEvent = eventExaminer.isLastEvent(currentSequence, currentEvent);
      if (eventExaminer.isInitialEvent(currentSequence, currentEvent)) {
        // First event of the key/window
        // What if it's a duplicate event - it will reset everything. Shall we drop/DLQ anything that's before
        // the processingState.lastOutputSequence?
        try {
          state = eventExaminer.createStateOnInitialEvent(currentEvent);
        } catch (Exception e) {
          // TODO: Handle exception in a better way - DLQ.
          // Initial state creator can be pretty heavy - remote calls, etc..
          throw new RuntimeException(e);
        }

        processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

        Result result = state.produceResult();
        if (result != null) {
          outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
          processingState.resultProduced();
        }

        // Nothing else to do. We will attempt to process buffered events later.
        return state;
      }

      if (processingState.isNextEvent(currentSequence)) {
        // Event matches expected sequence
        state = currentStateState.read();

        state.mutate(currentEvent);
        Result result = state.produceResult();
        if (result != null) {
          outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
          processingState.resultProduced();
        }
        processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

        return state;
      }

      // Event is not ready to be processed yet
      Instant eventTimestamp = Instant.ofEpochMilli(currentSequence);
      bufferedEventsState.add(TimestampedValue.of(currentEvent, eventTimestamp));
      processingState.eventBuffered(currentSequence, thisIsTheLastEvent);

      // This will signal that the state hasn't been mutated and we don't need to save it.
      return null;
    }

    /**
     * Process buffered events
     *
     * @param processingState
     * @param state
     * @param bufferedEventsState
     * @param outputReceiver
     * @param largeBatchEmissionTimer
     */
    private void processBufferedEvents(ProcessingState<EventKey> processingState,
        State state, OrderedListState<Event> bufferedEventsState,
        MultiOutputReceiver outputReceiver,
        Timer largeBatchEmissionTimer) {
      if (state == null) {
        // Only when the current event caused a state mutation and the state is passed to this
        // method should we attempt to process buffered events
        return;
      }

      if (!processingState.readyToProcessBufferedEvents()) {
        return;
      }

      if (exceededMaxResultCountForBundle(processingState, largeBatchEmissionTimer)) {
        // No point in trying to process buffered events
        return;
      }

      int recordCount = 0;
      Instant firstEventRead = null;

      Instant startRange = Instant.ofEpochMilli(processingState.getEarliestBufferedSequence());
      Instant endRange = Instant.ofEpochMilli(processingState.getLatestBufferedSequence() + 1);
      Instant endClearRange = null;

      // readRange is efficiently implemented and will bring records in batches
      Iterable<TimestampedValue<Event>> events = bufferedEventsState.readRange(startRange,
          endRange);

      Iterator<TimestampedValue<Event>> bufferedEventsIterator = events.iterator();
      while (bufferedEventsIterator.hasNext()) {
        TimestampedValue<Event> timestampedEvent = bufferedEventsIterator.next();
        Instant eventTimestamp = timestampedEvent.getTimestamp();
        long eventSequence = eventTimestamp.getMillis();
        if (recordCount++ == 0) {
          firstEventRead = eventTimestamp;
        }

        Event bufferedEvent = timestampedEvent.getValue();
        if (processingState.checkForDuplicateBatchedEvent(eventSequence)) {
          outputReceiver.get(unprocessedEventsTupleTag).output(KV.of(processingState.getKey(),
              KV.of(eventSequence, UnprocessedEvent.create(bufferedEvent, Reason.duplicate))));
          continue;
        }

        if (eventSequence > processingState.getLastOutputSequence() + 1) {
          processingState.foundSequenceGap(eventSequence);
          // Records will be cleared up to this element
          endClearRange = Instant.ofEpochMilli(eventSequence);
          break;
        }

        // This check needs to be done after we checked for sequence gap and before we
        // attempt to process the next element which can result in a new result.
        if (exceededMaxResultCountForBundle(processingState, largeBatchEmissionTimer)) {
          endClearRange = Instant.ofEpochMilli(eventSequence);
          break;
        }

        state.mutate(bufferedEvent);
        Result result = state.produceResult();
        if (result != null) {
          outputReceiver.get(mainOutputTupleTag).output(KV.of(processingState.getKey(), result));
          processingState.resultProduced();
        }
        processingState.processedBufferedEvent(eventSequence);
        // Remove this record also
        endClearRange = Instant.ofEpochMilli(eventSequence + 1);
      }

      bufferedEventsState.clearRange(startRange, endClearRange);
    }

    private boolean exceededMaxResultCountForBundle(ProcessingState<EventKey> processingState,
        Timer largeBatchEmissionTimer) {
      boolean exceeded = processingState.resultsProducedInBundle(numberOfResultsBeforeBundleStart)
          >= maxNumberOfResultsToProduce;
      if (exceeded) {
        LOG.info("Setting the timer to output next batch of events for key '"
            + processingState.getKey() + "'");
        // TODO: this work fine for global windows. Need to check what happens for other types of windows.
        // See GroupIntoBatches for examples on how to hold the timestamp.
        // TODO: test that on draining the pipeline all the results are still produced correctly.
        largeBatchEmissionTimer.offset(Duration.millis(1)).setRelative();
      }
      return exceeded;
    }

    @OnTimer(LARGE_BATCH_EMISSION_TIMER)
    public void onBatchEmission(OnTimerContext context,
        @StateId(BUFFERED_EVENTS) OrderedListState<Event> bufferedEventsState,
        @AlwaysFetched @StateId(PROCESSING_STATE) ValueState<ProcessingState<EventKey>> processingStatusState,
        @AlwaysFetched @StateId(MUTABLE_STATE) ValueState<State> currentStateState,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        OutputReceiver<KV<EventKey, Result>> resultReceiver, MultiOutputReceiver outputReceiver) {
      ProcessingState<EventKey> processingState = processingStatusState.read();
      if (processingState == null) {
        LOG.warn("Processing state is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }
      State state = currentStateState.read();
      if (state == null) {
        LOG.warn("Mutable state is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }

      LOG.debug("Starting to process batch for key '" + processingState.getKey() + "'");

      this.numberOfResultsBeforeBundleStart = processingState.getResultCount();

      processBufferedEvents(processingState, state, bufferedEventsState,
          outputReceiver, largeBatchEmissionTimer);

      saveStates(processingStatusState, processingState, currentStateState,
          state, outputReceiver, processingState.getKey(),
          // TODO: validate that this is correct.
          context.window().maxTimestamp());

      checkIfProcessingIsCompleted(processingState);
    }

    @OnTimer(STATUS_EMISSION_TIMER)
    @SuppressWarnings("unused")
    public void onStatusEmission(MultiOutputReceiver outputReceiver,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState,
        @StateId(PROCESSING_STATE) ValueState<ProcessingState<EventKey>> processingStateState) {

      ProcessingState<EventKey> currentState = processingStateState.read();
      if (currentState == null) {
        // This could happen if the state has been purged already during the draining.
        // It means that there is nothing that we can do and we just need to return.
        LOG.warn(
            "Current processing state is null in onStatusEmission() - most likely the pipeline is shutting down.");
        return;
      }

      emitProcessingStatus(currentState, outputReceiver, Instant.now());

      Boolean windowClosed = windowClosedState.read();
      if (!currentState.isProcessingCompleted()
          // Stop producing statuses if we are finished for a particular key
          && (windowClosed == null || !windowClosed)) {
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    @OnWindowExpiration
    public void onWindowExpiration(@StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState) {
      windowClosedState.write(true);
    }
  }
}