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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.Builder;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.ClearedBufferedEvents;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.QueriedBufferedEvents;
import org.apache.beam.sdk.extensions.ordered.ProcessingState.ProcessingStateCoder;
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
import org.apache.beam.sdk.transforms.ProcessFunction;
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
 * @param <EventT>
 * @param <KeyT>
 * @param <StateT>
 */
@AutoValue
@SuppressWarnings({"nullness", "TypeNameShadowing"})
public abstract class OrderedEventProcessor<EventT, KeyT, ResultT, StateT extends MutableState<EventT, ResultT>> extends
    PTransform<PCollection<KV<KeyT, KV<Long, EventT>>>, OrderedEventProcessorResult<KeyT, ResultT>> {

  public static final int DEFAULT_STATUS_UPDATE_FREQUENCY_SECONDS = 5;
  public static final boolean DEFAULT_PRODUCE_DIAGNOSTIC_EVENTS = false;
  private static final boolean DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT = false;
  public static final int DEFAULT_MAX_ELEMENTS_TO_OUTPUT = 10_000;

  /**
   * Default constructor method
   *
   * @param initialStateCreator creates the initial state type based on the first event
   * @param eventCoder          coder for the Event class
   * @param stateCoder          coder for the State class
   * @param keyTypeCoder        coder for the Key class
   * @param <EventType>
   * @param <KeyType>
   * @param <StateType>
   * @return
   */
  public static <EventType, KeyType, ResultType, StateType extends MutableState<EventType, ResultType>> OrderedEventProcessor<EventType, KeyType, ResultType, StateType> create(
      ProcessFunction<EventType, StateType> initialStateCreator,
      EventExaminer<EventType> eventExaminer, Coder<EventType> eventCoder,
      Coder<StateType> stateCoder, Coder<KeyType> keyTypeCoder, Coder<ResultType> resultTypeCoder) {
    // TODO: none of the values are marked as @Nullable and the transform will fail if nulls are provided. But need a better error messaging.
    return new AutoValue_OrderedEventProcessor<>(initialStateCreator, eventExaminer, eventCoder,
        stateCoder, keyTypeCoder, resultTypeCoder,
        DEFAULT_STATUS_UPDATE_FREQUENCY_SECONDS, DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT,
        DEFAULT_PRODUCE_DIAGNOSTIC_EVENTS, DEFAULT_MAX_ELEMENTS_TO_OUTPUT);
  }


  /**
   * Provide a custom status update frequency
   *
   * @param seconds
   * @return
   */

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> withStatusUpdateFrequencySeconds(
      int seconds) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventExaminer(), this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(),
        this.getResultCoder(), seconds,
        this.isProduceStatusUpdateOnEveryEvent(), this.isProduceDiagnosticEvents(),
        this.getMaxNumberOfResultsPerOutput());
  }

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> produceDiagnosticEvents(
      boolean produceDiagnosticEvents) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventExaminer(), this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(),
        this.getResultCoder(),
        this.getStatusUpdateFrequencySeconds(), this.isProduceStatusUpdateOnEveryEvent(),
        produceDiagnosticEvents, this.getMaxNumberOfResultsPerOutput());
  }

  /**
   * Notice that unless the status frequency update is set to 0 or negative number the status will
   * be produced on every event and with the specified frequency.
   *
   * @param produceDiagnosticEvents
   * @return
   */
  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> produceStatusUpdatesOnEveryEvent(
      boolean produceDiagnosticEvents) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventExaminer(), this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(),
        this.getResultCoder(),
        this.getStatusUpdateFrequencySeconds(), true, this.isProduceDiagnosticEvents(),
        this.getMaxNumberOfResultsPerOutput());
  }

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> withMaxResultsPerOutput(
      int maxResultsPerOutput) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventExaminer(), this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(),
        this.getResultCoder(),
        this.getStatusUpdateFrequencySeconds(), this.isProduceStatusUpdateOnEveryEvent(),
        this.isProduceDiagnosticEvents(), maxResultsPerOutput);
  }

  abstract ProcessFunction<EventT, StateT> getInitialStateCreator();

  abstract EventExaminer<EventT> getEventExaminer();

  abstract Coder<EventT> getEventCoder();

  abstract Coder<StateT> getStateCoder();

  abstract Coder<KeyT> getKeyCoder();

  abstract Coder<ResultT> getResultCoder();

  abstract int getStatusUpdateFrequencySeconds();

  abstract boolean isProduceStatusUpdateOnEveryEvent();

  abstract boolean isProduceDiagnosticEvents();

  abstract int getMaxNumberOfResultsPerOutput();

  @Override
  public OrderedEventProcessorResult expand(PCollection<KV<KeyT, KV<Long, EventT>>> input) {
    final TupleTag<KV<KeyT, ResultT>> mainOutput = new TupleTag<>("mainOutput") {
    };
    final TupleTag<KV<KeyT, OrderedProcessingStatus>> statusOutput = new TupleTag<>("status") {
    };

    final TupleTag<KV<KeyT, OrderedProcessingDiagnosticEvent>> diagnosticOutput = new TupleTag<>(
        "diagnostics") {
    };

    PCollectionTuple processingResult = input.apply(ParDo.of(
        new OrderedProcessorDoFn<>(getInitialStateCreator(), getEventExaminer(), getEventCoder(),
            getStateCoder(),
            getKeyCoder(), statusOutput,
            getStatusUpdateFrequencySeconds() <= 0 ? null
                : Duration.standardSeconds(getStatusUpdateFrequencySeconds()), diagnosticOutput,
            isProduceDiagnosticEvents(), isProduceStatusUpdateOnEveryEvent(),
            input.isBounded() == IsBounded.BOUNDED ? Integer.MAX_VALUE
                : getMaxNumberOfResultsPerOutput())).withOutputTags(mainOutput,
        TupleTagList.of(Arrays.asList(statusOutput, diagnosticOutput))));

    OrderedEventProcessorResult<KeyT, ResultT> result = new OrderedEventProcessorResult<>(
        input.getPipeline(),
        processingResult.get(mainOutput).setCoder(KvCoder.of(getKeyCoder(), getResultCoder())),
        mainOutput, processingResult.get(statusOutput)
        .setCoder(KvCoder.of(getKeyCoder(), getOrderedProcessingStatusCoder(input.getPipeline()))),
        statusOutput, processingResult.get(diagnosticOutput).setCoder(
        KvCoder.of(getKeyCoder(), getOrderedProcessingDiagnosticsCoder(input.getPipeline()))),
        diagnosticOutput);

    return result;
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

  private static Coder<OrderedProcessingDiagnosticEvent> getOrderedProcessingDiagnosticsCoder(
      Pipeline pipeline) {
    SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
    Coder<OrderedProcessingDiagnosticEvent> result;
    try {
      result = SchemaCoder.of(schemaRegistry.getSchema(OrderedProcessingDiagnosticEvent.class),
          TypeDescriptor.of(OrderedProcessingDiagnosticEvent.class),
          schemaRegistry.getToRowFunction(OrderedProcessingDiagnosticEvent.class),
          schemaRegistry.getFromRowFunction(OrderedProcessingDiagnosticEvent.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Main DoFn for processing ordered events
   *
   * @param <Event>
   * @param <Key>
   * @param <State>
   */
  static class OrderedProcessorDoFn<Event, Key, Result, State extends MutableState<Event, Result>> extends
      DoFn<KV<Key, KV<Long, Event>>, KV<Key, Result>> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedProcessorDoFn.class);

    private static final String PROCESSING_STATE = "processingState";
    private static final String MUTABLE_STATE = "mutableState";
    private static final String BUFFERED_EVENTS = "bufferedEvents";
    private static final String STATUS_EMISSION_TIMER = "statusTimer";
    private static final String LARGE_BATCH_EMISSION_TIMER = "largeBatchTimer";
    private static final String WINDOW_CLOSED = "windowClosed";
    private final ProcessFunction<Event, State> initialStateCreator;
    private final EventExaminer<Event> eventExaminer;

    @StateId(BUFFERED_EVENTS)
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<Event>> bufferedEventsSpec;

    @StateId(PROCESSING_STATE)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<ProcessingState<Key>>> processingStateSpec;

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

    private final TupleTag<KV<Key, OrderedProcessingStatus>> statusTupleTag;
    private final Duration statusUpdateFrequency;

    private final TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticEventsTupleTag;
    private final boolean produceDiagnosticEvents;
    private final boolean produceStatusUpdateOnEveryEvent;

    private final int maxNumberOfResultsToProduce;

    /**
     * Stateful DoFn for ordered processing.
     *
     * @param initialStateCreator
     * @param eventCoder
     * @param stateCoder
     * @param keyCoder
     * @param statusTupleTag
     * @param statusUpdateFrequency
     * @param diagnosticEventsTupleTag
     * @param produceDiagnosticEvents
     * @param produceStatusUpdateOnEveryEvent
     * @param maxNumberOfResultsToProduce
     */
    OrderedProcessorDoFn(ProcessFunction<Event, State> initialStateCreator,
        EventExaminer<Event> eventExaminer, Coder<Event> eventCoder,
        Coder<State> stateCoder, Coder<Key> keyCoder,
        TupleTag<KV<Key, OrderedProcessingStatus>> statusTupleTag, Duration statusUpdateFrequency,
        TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticEventsTupleTag,
        boolean produceDiagnosticEvents, boolean produceStatusUpdateOnEveryEvent,
        int maxNumberOfResultsToProduce) {
      this.initialStateCreator = initialStateCreator;
      this.eventExaminer = eventExaminer;
      this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
      this.mutableStateSpec = StateSpecs.value(stateCoder);
      this.processingStateSpec = StateSpecs.value(ProcessingStateCoder.of(keyCoder));
      this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
      this.statusTupleTag = statusTupleTag;
      this.statusUpdateFrequency = statusUpdateFrequency;
      this.diagnosticEventsTupleTag = diagnosticEventsTupleTag;
      this.produceDiagnosticEvents = produceDiagnosticEvents;
      this.produceStatusUpdateOnEveryEvent = produceStatusUpdateOnEveryEvent;
      this.maxNumberOfResultsToProduce = maxNumberOfResultsToProduce;
    }

    @ProcessElement
    public void processElement(
        @StateId(BUFFERED_EVENTS) OrderedListState<Event> bufferedEventsState,
        @AlwaysFetched @StateId(PROCESSING_STATE) ValueState<ProcessingState<Key>> processingStateState,
        @StateId(MUTABLE_STATE) ValueState<State> mutableStateState,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        @Element KV<Key, KV<Long, Event>> eventAndSequence,
        OutputReceiver<KV<Key, Result>> resultOutputter, MultiOutputReceiver outputReceiver,
        BoundedWindow window) {

      // TODO: should we make diagnostics generation optional?
      OrderedProcessingDiagnosticEvent.Builder diagnostics = OrderedProcessingDiagnosticEvent.builder();
      diagnostics.setProcessingTime(Instant.now());

      Key key = eventAndSequence.getKey();
      long sequence = eventAndSequence.getValue().getKey();
      Event event = eventAndSequence.getValue().getValue();

      diagnostics.setSequenceNumber(sequence);

      ProcessingState<Key> processingState = processingStateState.read();

      if (processingState == null) {
        // This is the first time we see this key/window pair
        processingState = new ProcessingState<>(key);
        // TODO: currently the parent transform doesn't support optional output of the statuses.
        if (statusUpdateFrequency != null) {
          // Set up the timer to produce the status of the processing on a regular basis
          statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
        }
      }

      processingState.recordReceived();
      diagnostics.setReceivedOrder(processingState.getRecordsReceived());

      State state = processNewEvent(sequence, event, processingState, mutableStateState,
          bufferedEventsState, resultOutputter, diagnostics);
      int numberOfRecordsOutput = 0;
      processBufferedEvents(processingState, numberOfRecordsOutput, state, bufferedEventsState,
          resultOutputter,
          largeBatchEmissionTimer, diagnostics);

      saveStatesAndOutputDiagnostics(processingStateState, processingState, mutableStateState,
          state, outputReceiver, diagnostics, key, window.maxTimestamp());
    }

    private void saveStatesAndOutputDiagnostics(
        ValueState<ProcessingState<Key>> processingStatusState,
        ProcessingState<Key> processingStatus, ValueState<State> currentStateState, State state,
        MultiOutputReceiver outputReceiver, Builder diagnostics, Key key, Instant windowTimestamp) {
      // There is always a change to the processing status
      processingStatusState.write(processingStatus);

      // Stored state may not have changes if the element was out of sequence.
      if (state != null) {
        currentStateState.write(state);
      }

      if (produceDiagnosticEvents) {
        outputReceiver.get(diagnosticEventsTupleTag).output(KV.of(key, diagnostics.build()));
      }

      if (produceStatusUpdateOnEveryEvent) {
        // During pipeline draining the window timestamp is set to a large value in the future.
        // Producing an event before that results in error, that's why this logic exist.
        Instant statusTimestamp = Instant.now();
        statusTimestamp =
            statusTimestamp.isAfter(windowTimestamp) ? statusTimestamp : windowTimestamp;

        outputReceiver.get(statusTupleTag).outputWithTimestamp(KV.of(processingStatus.getKey(),
            OrderedProcessingStatus.create(processingStatus.getLastOutputSequence(),
                processingStatus.getBufferedRecordCount(),
                processingStatus.getEarliestBufferedSequence(),
                processingStatus.getLatestBufferedSequence(),
                processingStatus.getRecordsReceived(),
                processingStatus.getDuplicates(),
                processingStatus.isLastEventReceived())), statusTimestamp);
      }
    }

    /**
     * Process the just received event
     *
     * @param currentSequence
     * @param currentEvent
     * @param processingState
     * @param currentStateState
     * @param bufferedEventsState
     * @param resultOutputter
     * @param diagnostics
     * @return
     */
    private State processNewEvent(long currentSequence, Event currentEvent,
        ProcessingState<Key> processingState, ValueState<State> currentStateState,
        OrderedListState<Event> bufferedEventsState,
        OutputReceiver<KV<Key, Result>> resultOutputter, Builder diagnostics) {

      if (processingState.hasAlreadyBeenProcessed(currentSequence)) {
        //-- TODO: add to statistics. Perhaps need DLQ for these events.
        return null;
      }

      State state;
      boolean thisIsTheLastEvent = eventExaminer.isLastEvent(currentSequence, currentEvent);
      if (eventExaminer.isInitialEvent(currentSequence, currentEvent)) {
        // First event of the key/window
        // TODO: do we need to validate that we haven't initialized the state already?
        // What if it's a duplicate event - it will reset everything. Shall we drop/DLQ anything that's before
        // the processingState.lastOutputSequence?
        try {
          state = initialStateCreator.apply(currentEvent);
        } catch (Exception e) {
          // TODO: Handle exception in a better way - DLQ.
          // Initial state creator can be pretty heavy - remote calls, etc..
          throw new RuntimeException(e);
        }

        processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

        Result result = state.produceResult();
        if (result != null) {
          resultOutputter.output(KV.of(processingState.getKey(), result));
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
          resultOutputter.output(KV.of(processingState.getKey(), result));
        }
        processingState.eventAccepted(currentSequence, thisIsTheLastEvent);

        return state;
      }

      // Event is not ready to be processed yet
      Instant eventTimestamp = Instant.ofEpochMilli(currentSequence);
      bufferedEventsState.add(TimestampedValue.of(currentEvent, eventTimestamp));
      processingState.eventBuffered(currentSequence, thisIsTheLastEvent);

      diagnostics.setEventBufferedTime(eventTimestamp);

      // This will signal that the state hasn't been mutated and we don't need to save it.
      return null;
    }

    /**
     * Process buffered events
     *
     * @param processingStatus
     * @param numberOfResultsProduced
     * @param state
     * @param bufferedEventsState
     * @param resultReceiver
     * @param largeBatchEmissionTimer
     * @param diagnostics
     */
    private void processBufferedEvents(ProcessingState<Key> processingStatus,
        int numberOfResultsProduced, State state,
        OrderedListState<Event> bufferedEventsState, OutputReceiver<KV<Key, Result>> resultReceiver,
        Timer largeBatchEmissionTimer, Builder diagnostics) {
      if (state == null) {
        // Only when the current event caused a state mutation and the state is passed to this
        // method should we attempt to process buffered events
        return;
      }

      if (!processingStatus.readyToProcessBufferedEvents()) {
        return;
      }

      int recordCount = 0;
      Instant firstEventRead = null;

      Instant startRange = Instant.ofEpochMilli(processingStatus.getEarliestBufferedSequence());
      // TODO: checks against lastBufferedSequence reaching Long.MAX_VALUE
      Instant endRange = Instant.ofEpochMilli(processingStatus.getLatestBufferedSequence() + 1);
      Instant endClearRange = null;

      // readRange is efficiently implemented and will bring records in batches
      Iterable<TimestampedValue<Event>> events = bufferedEventsState.readRange(startRange,
          endRange);

      // TODO: Draining events is a temporary workaround related to https://github.com/apache/beam/issues/28370
      // It can be removed once https://github.com/apache/beam/pull/28371 is available in a regular release, ETA 2.51.0
      boolean drainEvents = false;
      for (TimestampedValue<Event> timestampedEvent : events) {
        if (drainEvents) {
          continue;
        }
        Instant eventTimestamp = timestampedEvent.getTimestamp();
        if (recordCount++ == 0) {
          firstEventRead = eventTimestamp;
        }
        long eventSequence = eventTimestamp.getMillis();

        if (eventSequence > processingStatus.getLastOutputSequence() + 1) {
          processingStatus.foundSequenceGap(eventSequence);
          // Records will be cleared up to this element
          endClearRange = Instant.ofEpochMilli(eventSequence);
          drainEvents = true;
          continue;
//          break;
        }

        state.mutate(timestampedEvent.getValue());
        Result result = state.produceResult();
        if (result != null) {
          resultReceiver.output(KV.of(processingStatus.getKey(), result));
          ++numberOfResultsProduced;
        }
        processingStatus.processedBufferedEvent(eventSequence);
        // Remove this record also
        endClearRange = Instant.ofEpochMilli(eventSequence + 1);

        if (numberOfResultsProduced >= maxNumberOfResultsToProduce) {
          LOG.info("Setting the timer to output next batch of events for key '"
              + processingStatus.getKey() + "'");
          largeBatchEmissionTimer.offset(Duration.millis(1)).setRelative();
          drainEvents = true;
        }
      }

      // Temporarily disabled due to https://github.com/apache/beam/pull/28171
      // TODO: uncomment once available in a Beam release, ETA 2.51.0
//      bufferedEventsState.clearRange(startRange, endClearRange);

      diagnostics.setQueriedBufferedEvents(
          QueriedBufferedEvents.create(startRange, endRange, firstEventRead));
      diagnostics.setClearedBufferedEvents(ClearedBufferedEvents.create(startRange, endClearRange));
    }

    @OnTimer(LARGE_BATCH_EMISSION_TIMER)
    public void onLargeBatchEmissionContinuation(OnTimerContext context,
//        @AlwaysFetched
        @StateId(BUFFERED_EVENTS) OrderedListState<Event> bufferedEventsState,
        @AlwaysFetched @StateId(PROCESSING_STATE) ValueState<ProcessingState<Key>> processingStatusState,
        @AlwaysFetched @StateId(MUTABLE_STATE) ValueState<State> currentStateState,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        OutputReceiver<KV<Key, Result>> resultReceiver, MultiOutputReceiver outputReceiver) {
      ProcessingState<Key> processingStatus = processingStatusState.read();
      if (processingStatus == null) {
        LOG.warn("Processing status is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }
      State state = currentStateState.read();
      if (state == null) {
        LOG.warn("Mutable state is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }

      LOG.info("Starting to process batch for key '" + processingStatus.getKey() + "'");
      OrderedProcessingDiagnosticEvent.Builder diagnostics = OrderedProcessingDiagnosticEvent.builder();
      diagnostics.setProcessingTime(Instant.now());

      int numberOfResultsProduced = 0;
      processBufferedEvents(processingStatus, numberOfResultsProduced, state, bufferedEventsState,
          resultReceiver,
          largeBatchEmissionTimer, diagnostics);

      saveStatesAndOutputDiagnostics(processingStatusState, processingStatus, currentStateState,
          state, outputReceiver, diagnostics, processingStatus.getKey(),
          // TODO: validate that this is correct.
          context.window().maxTimestamp());
    }

    @OnTimer(STATUS_EMISSION_TIMER)
    @SuppressWarnings("unused")
    public void onStatusEmission(MultiOutputReceiver outputReceiver,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState,
        @StateId(PROCESSING_STATE) ValueState<ProcessingState<Key>> processingStatusState) {

      ProcessingState<Key> currentStatus = processingStatusState.read();
      if (currentStatus == null) {
        // This could happen if the state has been purged already during the draining.
        // It means that there is nothing that we can do and we just need to return.
        LOG.warn(
            "Current status is null in onStatusEmission() - most likely the pipeline is shutting down.");
        return;
      }
      outputReceiver.get(statusTupleTag).outputWithTimestamp(KV.of(currentStatus.getKey(),
          OrderedProcessingStatus.create(currentStatus.getLastOutputSequence(),
              currentStatus.getBufferedRecordCount(), currentStatus.getEarliestBufferedSequence(),
              currentStatus.getLatestBufferedSequence(), currentStatus.getRecordsReceived(),
              currentStatus.getDuplicates(),
              currentStatus.isLastEventReceived())), Instant.now());

      if (currentStatus.isProcessingCompleted()) {
        LOG.info("Processing for key '" + currentStatus.getKey() + "' is completed.");
        return;
      }
      Boolean windowClosed = windowClosedState.read();
      if (windowClosed == null || !windowClosed) {
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    @OnWindowExpiration
    public void onWindowExpiration(@StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState) {
      windowClosedState.write(true);
    }
  }
}