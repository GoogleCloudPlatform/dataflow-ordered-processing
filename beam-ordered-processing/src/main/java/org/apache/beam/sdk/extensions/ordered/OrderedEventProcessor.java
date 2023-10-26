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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.Builder;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.ClearedBufferedEvents;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingDiagnosticEvent.QueriedBufferedEvents;
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
public abstract class OrderedEventProcessor<EventT, KeyT, ResultT, StateT extends MutableState<EventT, ResultT>>
    extends
    PTransform<PCollection<KV<KeyT, KV<Long, EventT>>>, OrderedEventProcessorResult<KeyT, ResultT>> {

  public static final int DEFAULT_STATUS_UPDATE_FREQENCY_SECONDS = 5;
  public static final int DEFAULT_INITIAL_SEQUENCE_NUMBER = 1;
  public static final boolean DEFAULT_PRODUCE_DIAGNOSTIC_EVENTS = false;
  private static final boolean DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT = false;
  public static final int DEFAULT_MAX_ELEMENTS_TO_OUTPUT = 1000;

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
  public static <EventType, KeyType, ResultType, StateType extends
      MutableState<EventType, ResultType>> OrderedEventProcessor<EventType, KeyType, ResultType, StateType> create(
      ProcessFunction<EventType, StateType> initialStateCreator,
      Coder<EventType> eventCoder,
      Coder<StateType> stateCoder,
      Coder<KeyType> keyTypeCoder,
      Coder<ResultType> resultTypeCoder) {
    // TODO: none of the values are marked as @Nullable and the transform will fail if nulls are provided. But need a better error messaging.
    return new AutoValue_OrderedEventProcessor<>(
        initialStateCreator, eventCoder, stateCoder, keyTypeCoder, resultTypeCoder,
        DEFAULT_INITIAL_SEQUENCE_NUMBER, DEFAULT_STATUS_UPDATE_FREQENCY_SECONDS,
        DEFAULT_PRODUCE_STATUS_UPDATE_ON_EVERY_EVENT,
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
        this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(), this.getResultCoder(),
        this.getInitialSequenceNumber(),
        seconds, this.isProduceStatusUpdateOnEveryEvent(), this.isProduceDiagnosticEvents(),
        this.getMaxNumberOfResultsPerOutput());
  }


  /**
   * Provide a custom initial sequence number to start with
   *
   * @param initialSequence
   * @return
   */

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> withInitialSequence(
      long initialSequence) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(), this.getResultCoder(),
        initialSequence,
        this.getStatusUpdateFrequencySeconds(), this.isProduceStatusUpdateOnEveryEvent(),
        this.isProduceDiagnosticEvents(), this.getMaxNumberOfResultsPerOutput());
  }

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> produceDiagnosticEvents(
      boolean produceDiagnosticEvents) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(), this.getResultCoder(),
        this.getInitialSequenceNumber(),
        this.getStatusUpdateFrequencySeconds(), this.isProduceStatusUpdateOnEveryEvent(),
        produceDiagnosticEvents, this.getMaxNumberOfResultsPerOutput());
  }

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> produceStatusUpdatesOnEveryEvent(
      boolean produceDiagnosticEvents) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(), this.getResultCoder(),
        this.getInitialSequenceNumber(),
        this.getStatusUpdateFrequencySeconds(), true, this.isProduceDiagnosticEvents(),
        this.getMaxNumberOfResultsPerOutput());
  }

  public OrderedEventProcessor<EventT, KeyT, ResultT, StateT> withMaxResultsPerOutput(
      int maxResultsPerOutput) {
    return new AutoValue_OrderedEventProcessor<>(this.getInitialStateCreator(),
        this.getEventCoder(), this.getStateCoder(), this.getKeyCoder(), this.getResultCoder(),
        this.getInitialSequenceNumber(),
        this.getStatusUpdateFrequencySeconds(), this.isProduceStatusUpdateOnEveryEvent(),
        this.isProduceDiagnosticEvents(),
        maxResultsPerOutput);
  }

  abstract ProcessFunction<EventT, StateT> getInitialStateCreator();

  abstract Coder<EventT> getEventCoder();

  abstract Coder<StateT> getStateCoder();

  abstract Coder<KeyT> getKeyCoder();

  abstract Coder<ResultT> getResultCoder();

  abstract long getInitialSequenceNumber();

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

    PCollectionTuple processingResult = input.apply(ParDo.of(new OrderedProcessorDoFn<>(
        getInitialStateCreator(),
        getEventCoder(),
        getStateCoder(),
        getKeyCoder(),
        getInitialSequenceNumber(),
        statusOutput,
        getStatusUpdateFrequencySeconds() <= 0 ? null
            : Duration.standardSeconds(getStatusUpdateFrequencySeconds()),
        diagnosticOutput,
        isProduceDiagnosticEvents(),
        isProduceStatusUpdateOnEveryEvent(),
        input.isBounded() == IsBounded.BOUNDED ? Integer.MAX_VALUE
            : getMaxNumberOfResultsPerOutput())
    ).withOutputTags(mainOutput, TupleTagList.of(Arrays.asList(statusOutput, diagnosticOutput))));

    OrderedEventProcessorResult<KeyT, ResultT> result = new OrderedEventProcessorResult<>(
        input.getPipeline(),
        processingResult.get(mainOutput).setCoder(KvCoder.of(getKeyCoder(), getResultCoder())),
        mainOutput,
        processingResult.get(statusOutput).setCoder(
            KvCoder.of(getKeyCoder(), getOrderedProcessingStatusCoder(input.getPipeline()))),
        statusOutput,
        processingResult.get(diagnosticOutput).setCoder(
            KvCoder.of(getKeyCoder(), getOrderedProcessingDiagnosticsCoder(input.getPipeline()))),
        diagnosticOutput);

    return result;
  }

  private static Coder<OrderedProcessingStatus> getOrderedProcessingStatusCoder(Pipeline pipeline) {
    SchemaRegistry schemaRegistry = pipeline.getSchemaRegistry();
    Coder<OrderedProcessingStatus> result;
    try {
      result = SchemaCoder.of(
          schemaRegistry.getSchema(OrderedProcessingStatus.class),
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
      result = SchemaCoder.of(
          schemaRegistry.getSchema(OrderedProcessingDiagnosticEvent.class),
          TypeDescriptor.of(OrderedProcessingDiagnosticEvent.class),
          schemaRegistry.getToRowFunction(OrderedProcessingDiagnosticEvent.class),
          schemaRegistry.getFromRowFunction(OrderedProcessingDiagnosticEvent.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  /**
   * Class used to store the status of processing for a particular key.
   *
   * @param <KeyT>
   */
  private static class ProcessingStatus<KeyT> {

    private Long lastOutputSequence;
    private Long latestBufferedSequence;
    private Long earliestBufferedSequence;
    private long bufferedRecordCount;

    private long recordsReceived = 0;

    private KeyT key;

    public ProcessingStatus(KeyT key) {
      this.key = key;
      this.bufferedRecordCount = 0;
    }

    /**
     * Only to be used by the coder
     *
     * @param key
     * @param lastOutputSequence
     * @param earliestBufferedSequence
     * @param latestBufferedSequence
     * @param bufferedRecordCount
     */
    ProcessingStatus(KeyT key, Long lastOutputSequence, Long earliestBufferedSequence,
        Long latestBufferedSequence, long bufferedRecordCount, long recordsReceived) {
      this(key);
      this.lastOutputSequence = lastOutputSequence;
      this.earliestBufferedSequence = earliestBufferedSequence;
      this.latestBufferedSequence = latestBufferedSequence;
      this.bufferedRecordCount = bufferedRecordCount;
      this.recordsReceived = recordsReceived;
    }

    public Long getLastOutputSequence() {
      return lastOutputSequence;
    }

    public Long getLatestBufferedSequence() {
      return latestBufferedSequence;
    }

    public Long getEarliestBufferedSequence() {
      return earliestBufferedSequence;
    }

    public long getBufferedRecordCount() {
      return bufferedRecordCount;
    }

    public long getRecordsReceived() {
      return recordsReceived;
    }

    public KeyT getKey() {
      return key;
    }

    /**
     * Current event matched the sequence and was processed.
     *
     * @param sequence
     */
    public void eventAccepted(long sequence) {
      lastOutputSequence = sequence;
    }

    /**
     * New event added to the buffer
     *
     * @param sequenceNumber of the event
     */
    void eventBuffered(long sequenceNumber) {
      bufferedRecordCount++;
      latestBufferedSequence = Math.max(sequenceNumber, latestBufferedSequence == null ?
          Long.MIN_VALUE : latestBufferedSequence);
      earliestBufferedSequence = Math.min(sequenceNumber, earliestBufferedSequence == null ?
          Long.MAX_VALUE : earliestBufferedSequence);
    }

    /**
     * An event was processed and removed from the buffer.
     *
     * @param sequence of the processed event
     */
    public void processedBufferedEvent(long sequence) {
      bufferedRecordCount--;
      lastOutputSequence = sequence;

      if (bufferedRecordCount == 0) {
        earliestBufferedSequence = latestBufferedSequence = null;
      } else {
        // TODO: We don't know for sure that it's the earliest record - we would need to test that.
        earliestBufferedSequence = sequence + 1;
      }
    }

    /**
     * A set of records was pulled from the buffer, but it turned out that the element is not
     * sequential. Record this newest sequence number - it will prevent unnecessary batch
     * retrieval.
     *
     * @param newEarliestSequence
     */
    public void foundSequenceGap(long newEarliestSequence) {
      earliestBufferedSequence = newEarliestSequence;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProcessingStatus)) {
        return false;
      }
      ProcessingStatus<KeyT> that = (ProcessingStatus<KeyT>) o;
      return bufferedRecordCount == that.bufferedRecordCount && lastOutputSequence.equals(
          that.lastOutputSequence) && Objects.equals(latestBufferedSequence,
          that.latestBufferedSequence) && Objects.equals(earliestBufferedSequence,
          that.earliestBufferedSequence) && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(lastOutputSequence, latestBufferedSequence, earliestBufferedSequence,
          bufferedRecordCount, key);
    }
  }

  /**
   * Coder for the processing status
   *
   * @param <KeyT>
   */
  static class ProcessingStatusCoder<KeyT> extends Coder<ProcessingStatus<KeyT>> {

    private static final NullableCoder<Long> NULLABLE_LONG_CODER = NullableCoder.of(
        VarLongCoder.of());
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final VarIntCoder INTEGER_CODER = VarIntCoder.of();

    private Coder<KeyT> keyCoder;

    public static <KeyT> ProcessingStatusCoder<KeyT> of(Coder<KeyT> keyCoder) {
      ProcessingStatusCoder<KeyT> result = new ProcessingStatusCoder<>();
      result.keyCoder = keyCoder;
      return result;
    }

    @Override
    public void encode(ProcessingStatus<KeyT> value, OutputStream outStream) throws IOException {
      NULLABLE_LONG_CODER.encode(value.getLastOutputSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getEarliestBufferedSequence(), outStream);
      NULLABLE_LONG_CODER.encode(value.getLatestBufferedSequence(), outStream);
      LONG_CODER.encode(value.getBufferedRecordCount(), outStream);
      LONG_CODER.encode(value.getRecordsReceived(), outStream);
      keyCoder.encode(value.getKey(), outStream);
    }

    @Override
    public ProcessingStatus<KeyT> decode(InputStream inStream) throws IOException {
      Long lastOutputSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long earliestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      Long latestBufferedSequence = NULLABLE_LONG_CODER.decode(inStream);
      int bufferedRecordCount = INTEGER_CODER.decode(inStream);
      long recordsReceivedCount = LONG_CODER.decode(inStream);
      KeyT key = keyCoder.decode(inStream);

      return new ProcessingStatus<>(key, lastOutputSequence, earliestBufferedSequence,
          latestBufferedSequence, bufferedRecordCount, recordsReceivedCount);
    }

    @Override
    public List<? extends Coder<?>> getCoderArguments() {
      return List.of();
    }

    @Override
    public void verifyDeterministic() {
    }
  }

  /**
   * Main DoFn for processing ordered events
   *
   * @param <Event>
   * @param <Key>
   * @param <State>
   */
  static class OrderedProcessorDoFn<Event, Key, Result, State extends MutableState<Event, Result>>
      extends DoFn<KV<Key, KV<Long, Event>>, KV<Key, Result>> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderedProcessorDoFn.class);

    private static final String PROCESSING_STATUS_SPEC = "processingStatus";
    private static final String CURRENT_STATE = "currentState";
    private static final String BUFFERED_EVENTS = "bufferedEvents";
    private static final String STATUS_EMISSION_TIMER = "statusTimer";
    private static final String LARGE_BATCH_EMISSION_TIMER = "largeBatchTimer";
    private static final String WINDOW_CLOSED = "windowClosed";
    private final ProcessFunction<Event, State> initialStateCreator;

    @StateId(BUFFERED_EVENTS)
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<Event>> bufferedEventsSpec;

    @StateId(PROCESSING_STATUS_SPEC)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<ProcessingStatus<Key>>> processingStatusSpec;

    @SuppressWarnings("unused")
    @StateId(CURRENT_STATE)
    private final StateSpec<ValueState<State>> currentStateSpec;

    @StateId(WINDOW_CLOSED)
    @SuppressWarnings("unused")
    private final StateSpec<ValueState<Boolean>> windowClosedSpec;

    @TimerId(STATUS_EMISSION_TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec statusEmissionTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @TimerId(LARGE_BATCH_EMISSION_TIMER)
    @SuppressWarnings("unused")
    private final TimerSpec largeBatchEmissionTimer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    private final long initialSequenceValue;

    private final TupleTag<KV<Key, OrderedProcessingStatus>> statusTupleTag;
    private final Duration statusUpdateFrequency;

    private final TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticEventsTupleTag;
    private final boolean produceDiagnosticEvents;
    private final boolean produceStatusUpdateOnEveryEvent;

    private final int maxNumberOfResultsToProduce;

    OrderedProcessorDoFn(ProcessFunction<Event, State> initialState,
        Coder<Event> eventCoder,
        Coder<State> stateCoder,
        Coder<Key> keyCoder,
        long initialSequenceValue,
        TupleTag<KV<Key, OrderedProcessingStatus>> statusTupleTag,
        Duration statusUpdateFrequency,
        TupleTag<KV<Key, OrderedProcessingDiagnosticEvent>> diagnosticEventsTupleTag,
        boolean produceDiagnosticEvents,
        boolean produceStatusUpdateOnEveryEvent,
        int maxNumberOfResultsToProduce) {
      this.initialStateCreator = initialState;
      this.bufferedEventsSpec = StateSpecs.orderedList(eventCoder);
      this.currentStateSpec = StateSpecs.value(stateCoder);
      this.processingStatusSpec = StateSpecs.value(ProcessingStatusCoder.of(keyCoder));
      this.windowClosedSpec = StateSpecs.value(BooleanCoder.of());
      this.initialSequenceValue = initialSequenceValue;
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
        @AlwaysFetched
        @StateId(PROCESSING_STATUS_SPEC) ValueState<ProcessingStatus<Key>> processingStatusState,
        @StateId(CURRENT_STATE) ValueState<State> currentStateState,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        @Element KV<Key, KV<Long, Event>> eventAndSequence,
        OutputReceiver<KV<Key, Result>> resultOutputter,
        MultiOutputReceiver outputReceiver,
        BoundedWindow window) {

      // TODO: should we make diagnostics generation optional?
      OrderedProcessingDiagnosticEvent.Builder diagnostics = OrderedProcessingDiagnosticEvent.builder();
      diagnostics.setProcessingTime(Instant.now());

      Key key = eventAndSequence.getKey();
      long sequence = eventAndSequence.getValue().getKey();
      Event event = eventAndSequence.getValue().getValue();

      diagnostics.setSequenceNumber(sequence);

      ProcessingStatus<Key> processingStatus = processingStatusState.read();

      if (processingStatus == null) {
        // This is the first time we see this key/window pair
        processingStatus = new ProcessingStatus<>(key);
        // TODO: currently the parent transform doesn't support optional output of the statuses.
        if (statusUpdateFrequency != null) {
          // Set up the timer to produce the status of the processing on a regular basis
          statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
        }
      }

      processingStatus.recordsReceived++;
      diagnostics.setReceivedOrder(processingStatus.recordsReceived);

      State state = processNewEvent(sequence, event, processingStatus, currentStateState,
          bufferedEventsState, resultOutputter, diagnostics);
      processBufferedEvents(processingStatus, state, bufferedEventsState, resultOutputter,
          largeBatchEmissionTimer, diagnostics);

      saveStatesAndOutputDiagnostics(processingStatusState, processingStatus,
          currentStateState,
          state, outputReceiver, diagnostics, key,
          window.maxTimestamp());
    }

    private void saveStatesAndOutputDiagnostics(
        ValueState<ProcessingStatus<Key>> processingStatusState,
        ProcessingStatus<Key> processingStatus,
        ValueState<State> currentStateState, State state,
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

        outputReceiver.get(statusTupleTag).outputWithTimestamp(
            KV.of(processingStatus.getKey(),
                OrderedProcessingStatus.create(
                    processingStatus.lastOutputSequence,
                    processingStatus.bufferedRecordCount,
                    processingStatus.earliestBufferedSequence,
                    processingStatus.latestBufferedSequence,
                    processingStatus.recordsReceived)),
            statusTimestamp);
      }
    }

    /**
     * Process the just received event
     *
     * @param currentSequence
     * @param currentEvent
     * @param processingStatus
     * @param currentStateState
     * @param bufferedEventsState
     * @param resultOutputter
     * @param diagnostics
     * @return
     */
    private State processNewEvent(long currentSequence, Event currentEvent,
        ProcessingStatus<Key> processingStatus,
        ValueState<State> currentStateState,
        OrderedListState<Event> bufferedEventsState,
        OutputReceiver<KV<Key, Result>> resultOutputter, Builder diagnostics) {
      State state = null;
      // TODO: it's possible that we would need to modify this logic and let the event itself determine if this is
      // the initial state.
      if (currentSequence == initialSequenceValue) {
        // First event of the key/window
        // TODO: do we need to validate that we haven't initialized the state already?
        // What if it's a duplicate event - it will reset everything. Shall we drop/DLQ anything that's before
        // the processingStatus.lastOutputSequence?
        try {
          state = initialStateCreator.apply(currentEvent);
        } catch (Exception e) {
          // TODO: Handle exception in a better way - DLQ.
          // Initial state creator can be pretty heavy - remote calls, etc..
          throw new RuntimeException(e);
        }
        processingStatus.eventAccepted(currentSequence);

        Result result = state.produceResult();
        if (result != null) {
          resultOutputter.output(KV.of(processingStatus.key, result));
        }

        // Nothing else to do. We will attempt to process buffered events later.
        return state;
      }

      if (processingStatus.lastOutputSequence != null
          && currentSequence == processingStatus.lastOutputSequence + 1) {
        // Event matches expected sequence
        state = currentStateState.read();

        state.mutate(currentEvent);
        Result result = state.produceResult();
        if (result != null) {
          resultOutputter.output(KV.of(processingStatus.getKey(), result));
        }
        processingStatus.eventAccepted(currentSequence);

        return state;
      }

      // Event is not ready to be processed yet
      Instant eventTimestamp = Instant.ofEpochMilli(currentSequence);
      bufferedEventsState.add(TimestampedValue.of(currentEvent, eventTimestamp));
      processingStatus.eventBuffered(currentSequence);

      diagnostics.setEventBufferedTime(eventTimestamp);

      // This will signal that the state hasn't been mutated and we don't need to save it.
      return null;
    }

    /**
     * Process buffered events
     *
     * @param processingStatus
     * @param state
     * @param bufferedEventsState
     * @param resultReceiver
     * @param largeBatchEmissionTimer
     * @param diagnostics
     */
    private void processBufferedEvents(ProcessingStatus<Key> processingStatus, State state,
        OrderedListState<Event> bufferedEventsState,
        OutputReceiver<KV<Key, Result>> resultReceiver, Timer largeBatchEmissionTimer,
        Builder diagnostics) {
      if (state == null) {
        // Only when the current event caused a state mutation and the state is passed to this
        // method should we attempt to process buffered events
        return;
      }
      if (processingStatus.bufferedRecordCount == 0) {
        // Nothing is buffered
        return;
      }

      // If we know upfront that the earliest record won't be sequential - break out right away
      if (processingStatus.earliestBufferedSequence > processingStatus.lastOutputSequence + 1) {
        return;
      }

      Instant endClearRange = null;

      int recordCount = 0;
      Instant firstEventRead = null;

      Instant startRange;
      Instant endRange;
      while (true) {
        startRange = Instant.ofEpochMilli(processingStatus.earliestBufferedSequence);
        // TODO: checks against lastBufferedSequence reaching Long.MAX_VALUE
        long lastSequenceToRead = calculateLastSequenceToRead(processingStatus,
            maxNumberOfResultsToProduce);
        endRange = Instant.ofEpochMilli(lastSequenceToRead);

        Iterable<TimestampedValue<Event>> events = bufferedEventsState.readRange(startRange,
            endRange);

        // TODO: Draining events is a temporary workaround issues related to https://github.com/apache/beam/issues/28370
        // It can be removed once https://github.com/apache/beam/pull/28371 is available in a regular release, ETA 2.51.0
        boolean drainEvents = false;
        int numberOfResultsProduced = 0;
        for (TimestampedValue<Event> timestampedEvent : events) {
          if (drainEvents) {
            continue;
          }
          Instant eventTimestamp = timestampedEvent.getTimestamp();
          if (recordCount++ == 0) {
            firstEventRead = eventTimestamp;
          }
          long eventSequence = eventTimestamp.getMillis();

          if (eventSequence > processingStatus.lastOutputSequence + 1) {
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
            resultReceiver.output(KV.of(processingStatus.key, result));
            ++numberOfResultsProduced;
          }
          processingStatus.processedBufferedEvent(eventSequence);
          // Remove this record also
          endClearRange = Instant.ofEpochMilli(eventSequence + 1);

          if (numberOfResultsProduced >= maxNumberOfResultsToProduce) {
//          Instant timeForNextRun = Instant.now().plus(Duration.standardSeconds(1));
            LOG.info("Setting the timer to output next batch of events for key "
                + processingStatus.getKey());
            largeBatchEmissionTimer.offset(Duration.millis(1)).setRelative();
            drainEvents = true;
          }
        }
        if (recordCount > 0) {
          break;
        }
        // Nothing was read from the range - meaning that there was a sequence gap larger than
        // the max number of records to produce
        processingStatus.foundSequenceGap(lastSequenceToRead);
      }

      // Temporarily disabled due to https://github.com/apache/beam/pull/28171
      // TODO: uncomment once available in a Beam release, ETA 2.51.0
//      bufferedEventsState.clearRange(startRange, endClearRange);

      diagnostics.setQueriedBufferedEvents(
          QueriedBufferedEvents.create(startRange, endRange, firstEventRead));
      diagnostics.setClearedBufferedEvents(
          ClearedBufferedEvents.create(startRange, endClearRange));
    }

    private long calculateLastSequenceToRead(ProcessingStatus<Key> processingStatus,
        long maxNumberOfResultsToProduce) {
      long result = processingStatus.latestBufferedSequence + 1;

      //--- Buffered results can have gaps in sequences, but the processing will stop on a gap anyway
      return Math.min(
          processingStatus.earliestBufferedSequence +
              // This logic will prevent numeric overflow at the very end of the Long's range
              Math.min(maxNumberOfResultsToProduce, processingStatus.bufferedRecordCount),
          result);
    }

    @OnTimer(LARGE_BATCH_EMISSION_TIMER)
    public void onLargeBatchEmissionContinuation(
        OnTimerContext context,
//        @AlwaysFetched
        @StateId(BUFFERED_EVENTS) OrderedListState<Event> bufferedEventsState,
        @AlwaysFetched
        @StateId(PROCESSING_STATUS_SPEC) ValueState<ProcessingStatus<Key>> processingStatusState,
        @AlwaysFetched
        @StateId(CURRENT_STATE) ValueState<State> currentStateState,
        @TimerId(LARGE_BATCH_EMISSION_TIMER) Timer largeBatchEmissionTimer,
        OutputReceiver<KV<Key, Result>> resultReceiver,
        MultiOutputReceiver outputReceiver
    ) {
      ProcessingStatus<Key> processingStatus = processingStatusState.read();
      if (processingStatus == null) {
        LOG.warn("Processing status is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }
      State state = currentStateState.read();
      if (state == null) {
        LOG.warn("Mutable state is empty. Ignore it if the pipeline is being cancelled.");
        return;
      }

      OrderedProcessingDiagnosticEvent.Builder diagnostics = OrderedProcessingDiagnosticEvent.builder();
      diagnostics.setProcessingTime(Instant.now());

      processBufferedEvents(processingStatus, state, bufferedEventsState, resultReceiver,
          largeBatchEmissionTimer, diagnostics);

      saveStatesAndOutputDiagnostics(processingStatusState, processingStatus,
          currentStateState, state, outputReceiver, diagnostics, processingStatus.getKey(),
          // TODO: validate that this is correct.
          context.window().maxTimestamp());
    }

    @OnTimer(STATUS_EMISSION_TIMER)
    @SuppressWarnings("unused")
    public void onStatusEmission(
        MultiOutputReceiver outputReceiver,
        @TimerId(STATUS_EMISSION_TIMER) Timer statusEmissionTimer,
        @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState,
        @StateId(PROCESSING_STATUS_SPEC) ValueState<ProcessingStatus<Key>> processingStatusState) {

      ProcessingStatus<Key> currentStatus = processingStatusState.read();
      if (currentStatus == null) {
        // This could happen if the state has been purged already during the draining.
        // It means that there is nothing that we can do and we just need to return.
        LOG.warn(
            "Current status is null in onStatusEmission() - most likely the pipeline is shutting down.");
        return;
      }
      outputReceiver.get(statusTupleTag).outputWithTimestamp(
          KV.of(currentStatus.getKey(),
              OrderedProcessingStatus.create(
                  currentStatus.lastOutputSequence,
                  currentStatus.bufferedRecordCount,
                  currentStatus.earliestBufferedSequence,
                  currentStatus.latestBufferedSequence,
                  currentStatus.recordsReceived)),
          Instant.now());

      Boolean windowClosed = windowClosedState.read();
      if (windowClosed == null || !windowClosed) {
        statusEmissionTimer.offset(statusUpdateFrequency).setRelative();
      }
    }

    @OnWindowExpiration
    public void onWindowExpiration(
        @StateId(WINDOW_CLOSED) ValueState<Boolean> windowClosedState) {
      windowClosedState.write(true);
    }
  }
}