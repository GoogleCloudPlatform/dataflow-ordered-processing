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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OrderedEventProcessorTest {

  @Rule
  public final transient TestPipeline p = TestPipeline.create();

  @DefaultCoder(StringBufferStateCoder.class)
  public static class StringBufferState implements MutableState<String, String> {

    private int emissionFrequency = 1;
    private long currentlyEmittedElementNumber = 0L;

    public StringBufferState(String initialEvent, int emissionFrequency) {
      this.emissionFrequency = emissionFrequency;
      mutate(initialEvent);
    }

    final private StringBuilder sb = new StringBuilder();

    @Override
    public void mutate(String mutation) {
      sb.append(mutation);
    }

    @Override
    public String produceResult() {
      return currentlyEmittedElementNumber++ % emissionFrequency == 0 ? sb.toString() : null;
    }

    public String toString() {
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof StringBufferState)) {
        return false;
      }
      StringBufferState that = (StringBufferState) o;
      return emissionFrequency == that.emissionFrequency
          && currentlyEmittedElementNumber == that.currentlyEmittedElementNumber && sb.toString()
          .equals(that.sb.toString());
    }

    @Override
    public int hashCode() {
      return Objects.hash(sb);
    }
  }

  public static class StringBufferStateCoder extends Coder<StringBufferState> {

    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    private static final Coder<Long> LONG_CODER = VarLongCoder.of();
    private static final Coder<Integer> INT_CODER = VarIntCoder.of();

    @Override
    public void encode(StringBufferState value,
        @UnknownKeyFor @NonNull @Initialized OutputStream outStream)
        throws IOException {
      INT_CODER.encode(value.emissionFrequency, outStream);
      LONG_CODER.encode(value.currentlyEmittedElementNumber, outStream);
      STRING_CODER.encode(value.sb.toString(), outStream);
    }

    @Override
    public StringBufferState decode(@UnknownKeyFor @NonNull @Initialized InputStream inStream)
        throws IOException {
      int emissionFrequency = INT_CODER.decode(inStream);
      long currentlyEmittedElementNumber = LONG_CODER.decode(inStream);
      String decoded = STRING_CODER.decode(inStream);
      StringBufferState result = new StringBufferState(decoded, emissionFrequency);
      result.currentlyEmittedElementNumber = currentlyEmittedElementNumber;
      return result;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized List<? extends @UnknownKeyFor @NonNull @Initialized Coder<@UnknownKeyFor @NonNull @Initialized ?>> getCoderArguments() {
      return List.of();
    }

    @Override
    public void verifyDeterministic() {

    }

    @Override
    public boolean consistentWithEquals() {
      return true;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized Object structuralValue(StringBufferState value) {
      return super.structuralValue(value);
    }
  }

  static class StateInitializer implements ProcessFunction<String, StringBufferState> {

    private final int emissionFrequency;

    StateInitializer(int frequency) {
      emissionFrequency = frequency;
    }

    @Override
    public StringBufferState apply(String input)
        throws @UnknownKeyFor @NonNull @Initialized Exception {
      return new StringBufferState(
          input,
          emissionFrequency);
    }
  }

  static class StringBufferEventExaminer implements EventExaminer<String> {

    private final long initialSequence;

    public StringBufferEventExaminer(long initialSequence) {
      this.initialSequence = initialSequence;
    }

    @Override
    public boolean isInitialEvent(long sequenceNumber, String input) {
      return sequenceNumber == initialSequence;
    }

    @Override
    public boolean isLastEvent(long sequenceNumber, String input) {
      // TODO: implement and test
      return false;
    }
  }


  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class Event implements Serializable {

    public static Event create(long sequence, String id, String value) {
      return new AutoValue_OrderedEventProcessorTest_Event(sequence, id, value);
    }

    public abstract long getSequence();

    public abstract String getId();

    public abstract String getValue();
  }

  static class MapEventsToKV extends DoFn<Event, KV<String, KV<Long, String>>> {

    @ProcessElement
    public void convert(@Element Event event,
        OutputReceiver<KV<String, KV<Long, String>>> outputReceiver) {
      outputReceiver.output(KV.of(event.getId(), KV.of(event.getSequence(), event.getValue())));
    }
  }

  static class MapStringBufferStateToString extends
      DoFn<KV<String, StringBufferState>, KV<String, String>> {

    @ProcessElement
    public void map(@Element KV<String, StringBufferState> element,
        OutputReceiver<KV<String, String>> outputReceiver) {
      outputReceiver.output(KV.of(element.getKey(), element.getValue().toString()));
    }
  }

  @Test
  public void testPerfectOrderingProcessing() throws CannotProvideCoderException {
    testProcessing(new Event[]{Event.create(0, "id-1", "a"), Event.create(1, "id-1", "b"),
            Event.create(2, "id-1", "c"), Event.create(3, "id-1", "d"), Event.create(0, "id-2", "a"),
            Event.create(1, "id-2", "b"),},
        new KV[]{KV.of("id-1", OrderedProcessingStatus.create(3L, 0, null, null, 4, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 0, false)),},
        new KV[]{KV.of("id-1", "a"), KV.of("id-1", "ab"), KV.of("id-1", "abc"),
            KV.of("id-1", "abcd"), KV.of("id-2", "a"), KV.of("id-2", "ab"),}, 1, 0, 1000, false);
  }

  @Test
  public void testOutOfSequenceProcessing() throws CannotProvideCoderException {
    testProcessing(new Event[]{
            Event.create(2, "id-1", "c"),
            Event.create(1, "id-1", "b"),
            Event.create(0, "id-1", "a"),
            Event.create(3, "id-1", "d"),

            Event.create(1, "id-2", "b"),
            Event.create(2, "id-2", "c"),
            Event.create(4, "id-2", "e"),
            Event.create(0, "id-2", "a"),
            Event.create(3, "id-2", "d")
        },
        new KV[]{
            KV.of("id-1", OrderedProcessingStatus.create(3L, 0, null, null, 4, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(4L, 0, null, null, 5, 0, false))},
        new KV[]{
            KV.of("id-1", "a"),
            KV.of("id-1", "ab"),
            KV.of("id-1", "abc"),
            KV.of("id-1", "abcd"),
            KV.of("id-2", "a"),
            KV.of("id-2", "ab"),
            KV.of("id-2", "abc"),
            KV.of("id-2", "abcd"),
            KV.of("id-2", "abcde"),
        }, 1, 0, 1000, false);
  }

  @Test
  public void testUnfinishedProcessing() throws CannotProvideCoderException {
    testProcessing(new Event[]{Event.create(2, "id-1", "c"),
//   Excluded                     Event.create(1, "id-1", "b"),
            Event.create(0, "id-1", "a"), Event.create(3, "id-1", "d"), Event.create(0, "id-2", "a"),
            Event.create(1, "id-2", "b"),},
        new KV[]{KV.of("id-1", OrderedProcessingStatus.create(0L, 2, 2L, 3L, 3, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 0, false))},
        new KV[]{KV.of("id-1", "a"), KV.of("id-2", "a"), KV.of("id-2", "ab")}, 1, 0, 1000, false);
  }

  @Test
  public void testHandlingOfDuplicateSequences() throws CannotProvideCoderException {
    testProcessing(new Event[]{
            Event.create(0, "id-1", "a"),
            Event.create(1, "id-1", "b"),
            Event.create(2, "id-1", "c"),
            Event.create(3, "id-1", "d"),
            // Duplicates
            Event.create(1, "id-1", "b"),
            Event.create(3, "id-1", "z"),
        },
        new KV[]{
            KV.of("id-1", OrderedProcessingStatus.create(3L, 0, null, null, 6, 2, false))
        },
        new KV[]{
            KV.of("id-1", "a"),
            KV.of("id-1", "ab"),
            KV.of("id-1", "abc"),
            KV.of("id-1", "abcd"),
        }, 1, 0, 1000, false);
  }

  @Test
  public void testProcessingWithEveryOtherResultEmission() throws CannotProvideCoderException {
    testProcessing(new Event[]{
            Event.create(2, "id-1", "c"),
            Event.create(1, "id-1", "b"),
            Event.create(0, "id-1", "a"),
            Event.create(3, "id-1", "d"),
            Event.create(0, "id-2", "a"),
            Event.create(1, "id-2", "b"),},
        new KV[]{KV.of("id-1", OrderedProcessingStatus.create(3L, 0, null, null, 4, 0, false)),
            KV.of("id-2", OrderedProcessingStatus.create(1L, 0, null, null, 2, 0, false))},
        new KV[]{KV.of("id-1", "a"),
//  Skipped                      KV.of("id-1", "ab"),
            KV.of("id-1", "abc"),
//  Skipped                      KV.of("id-1", "abcd"),
            KV.of("id-2", "a"),
//  Skipped                      KV.of("id-2", "ab")
        }, 2, 0, 1000, false);
  }

  @Test
  public void testLargeBufferedOutputInTimer() throws CannotProvideCoderException {
    int maxResultsPerOutput = 100;

    // Array of sequences starting with 2 and the last element - 1.
    // Output will be buffered until the last event arrives
    long[] sequences = new long[maxResultsPerOutput * 3];
    for (int i = 0; i < sequences.length - 1; i++) {
      sequences[i] = i + 2;
    }
    sequences[sequences.length - 1] = 1;

    List<Event> events = new ArrayList<>(sequences.length);
    List<KV<String, String>> expectedOutput = new ArrayList<>(sequences.length);
    List<KV<String, OrderedProcessingStatus>> expectedStatuses = new ArrayList<>(
        sequences.length + 10);

    StringBuilder output = new StringBuilder();
    String outputPerElement = ".";
    String key = "id-1";

    int bufferedEventCount = 0;

    for (long sequence : sequences) {
      ++bufferedEventCount;

      events.add(Event.create(sequence, key, outputPerElement));
      output.append(outputPerElement);
      expectedOutput.add(KV.of(key, output.toString()));

      if (bufferedEventCount < sequences.length) {
        // Last event will result in a batch of events being produced. That's why it's excluded here.
        expectedStatuses.add(KV.of(key,
            OrderedProcessingStatus.create(null, bufferedEventCount, 2L, sequence,
                bufferedEventCount, 0, false)));
      }
    }

    // Statuses produced by the batched processing
    for (int i = 1 + maxResultsPerOutput; i < sequences.length; i += maxResultsPerOutput) {
      long lastOutputSequence = i;
      expectedStatuses.add(KV.of(key,
          OrderedProcessingStatus.create(lastOutputSequence, sequences.length - lastOutputSequence,
              lastOutputSequence + 1, (long) sequences.length, sequences.length, 0, false)));
    }

    //-- Final status - indicates that everything has been fully processed
    expectedStatuses.add(KV.of(key,
        OrderedProcessingStatus.create((long) sequences.length, 0, null, null,
            sequences.length, 0, false)));

    testProcessing(events.toArray(Event[]::new), expectedStatuses.toArray(KV[]::new),
        expectedOutput.toArray(KV[]::new), 1,
        1L /* This dataset assumes 1 as the starting sequence */, maxResultsPerOutput, true);
  }

  @Test
  public void testSequenceGapProcessingInBufferedOutput() throws CannotProvideCoderException {
    int maxResultsPerOutput = 3;

    long[] sequences = new long[]{2, 3,
        /* Once 1 arrives, 1,2,3 will be produced, with earliestBuffered listed as 4.
        Next read of maxResultsPerOutput will read 3 up to 7, which wll return nothing and
        will require an extra read.
         */
        7, 8, 9, 10, 1, 4, 5, 6};

    List<Event> events = new ArrayList<>(sequences.length);
    List<KV<String, String>> expectedOutput = new ArrayList<>(sequences.length);

    StringBuilder output = new StringBuilder();
    String outputPerElement = ".";
    String key = "id-1";

    for (long sequence : sequences) {
      events.add(Event.create(sequence, key, outputPerElement));
      output.append(outputPerElement);
      expectedOutput.add(KV.of(key, output.toString()));
    }

    int numberOfReceivedEvents = 0;
    KV<String, OrderedProcessingStatus>[] expectedStatuses = new KV[]{KV.of(key,
        OrderedProcessingStatus.create(null, 1, 2L, 2L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(null, 2, 2L, 3L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(null, 3, 2L, 7L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(null, 4, 2L, 8L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(null, 5, 2L, 9L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(null, 6, 2L, 10L, ++numberOfReceivedEvents, 0, false)),
        // --- 1 has appeared and caused the batch to be sent out.
        KV.of(key,
            OrderedProcessingStatus.create(3L, 4, 7L, 10L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(4L, 4, 7L, 10L, ++numberOfReceivedEvents, 0, false)),
        KV.of(key,
            OrderedProcessingStatus.create(5L, 4, 7L, 10L, ++numberOfReceivedEvents, 0, false)),
        // --- 6 came and 6, 7, and 8 got output
        KV.of(key,
            OrderedProcessingStatus.create(9L, 1, 10L, 10L, ++numberOfReceivedEvents, 0, false)),
        // Last timer run produces the final status. Number of received events doesn't increase,
        // this is the result of a timer processing
        KV.of(key,
            OrderedProcessingStatus.create(10L, 0, null, null, numberOfReceivedEvents, 0, false)),};

    testProcessing(events.toArray(Event[]::new), expectedStatuses,
        expectedOutput.toArray(KV[]::new), 1,
        1L /* This dataset assumes 1 as the starting sequence */, maxResultsPerOutput, true);
  }

  private void testProcessing(Event[] events, KV[] expectedStatuses, KV[] expectedOutput,
      int emissionFrequency, long initialSequence, int maxResultsPerOutput,
      boolean produceStatusOnEveryEvent)
      throws @UnknownKeyFor @NonNull @Initialized CannotProvideCoderException {
    Instant now = Instant.now().minus(Duration.standardMinutes(20));
    TestStream.Builder<Event> messageFlow = TestStream.create(
        p.getCoderRegistry().getCoder(Event.class)).advanceWatermarkTo(now);

    int delayInMilliseconds = 0;
    for (Event e : events) {
      messageFlow = messageFlow.advanceWatermarkTo(now.plus(Duration.millis(++delayInMilliseconds)))
          .addElements(e);
    }

    // Needed to force the processing time based timers.
    messageFlow = messageFlow.advanceProcessingTime(Duration.standardMinutes(15));

    PCollection<KV<String, KV<Long, String>>> input = p.apply("Create Events",
        messageFlow.advanceWatermarkToInfinity()).apply("To KV", ParDo.of(new MapEventsToKV()));

    Coder<StringBufferState> stateCoder = p.getCoderRegistry().getCoder(StringBufferState.class);
    Coder<String> eventCoder = StringUtf8Coder.of();
    Coder<String> keyCoder = StringUtf8Coder.of();
    Coder<String> resultCoder = StringUtf8Coder.of();

    OrderedEventProcessor<String, String, String, StringBufferState> orderedEventProcessor = OrderedEventProcessor.create(
            new StateInitializer(emissionFrequency), new StringBufferEventExaminer(initialSequence),
            eventCoder, stateCoder, keyCoder, resultCoder)
        .withMaxResultsPerOutput(maxResultsPerOutput);

    if (produceStatusOnEveryEvent) {
      orderedEventProcessor = orderedEventProcessor.produceStatusUpdatesOnEveryEvent(true)
          .withStatusUpdateFrequencySeconds(-1);
    } else {
      orderedEventProcessor = orderedEventProcessor.withStatusUpdateFrequencySeconds(20);
    }

    OrderedEventProcessorResult<String, String> orderedProcessing = input.apply("Process Events",
        orderedEventProcessor);

    PCollection<KV<String, String>> output = orderedProcessing.output();
    PAssert.that("Output", output).containsInAnyOrder(expectedOutput);

    PCollection<KV<String, OrderedProcessingStatus>> processingStatuses = orderedProcessing.processingStatuses();
    PAssert.that("Statuses", processingStatuses).containsInAnyOrder(expectedStatuses);

    p.run();
  }
}

