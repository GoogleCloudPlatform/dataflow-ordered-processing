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

import java.util.Arrays;
import java.util.Iterator;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.state.OrderedListState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// TODO: test if it's fixed and remove this class. It's only been used to illustrate a bug.
public class OrderedListStateIT {

  private static final String DATAFLOW_TEMP_BUCKET = "DATAFLOW_TEMP_BUCKET";
  private static final Long SINGLE_KEY = 1L;

  @Rule
  public TestPipeline p = TestPipeline.create();

  static class MapInstantToKV extends DoFn<Instant, KV<Long, Instant>> {

    @ProcessElement
    public void convert(@Element Instant instant,
        OutputReceiver<KV<Long, Instant>> outputReceiver) {
      outputReceiver.output(KV.of(SINGLE_KEY, instant));
    }
  }

  static class StatefulParDo extends DoFn<KV<Long, Instant>, String> {

    public static final Logger LOG = LoggerFactory.getLogger(StatefulParDo.class);

    @StateId("ordered_list_state")
    @SuppressWarnings("unused")
    private final StateSpec<OrderedListState<String>> bufferedInstancesSpec = StateSpecs.orderedList(
        StringUtf8Coder.of());

    @StateId("initial_instant")
    private final StateSpec<ValueState<Instant>> initialInstantSpec = StateSpecs.value(
        InstantCoder.of());

    @StateId("element_count")
    private final StateSpec<ValueState<Integer>> elementCountSpec = StateSpecs.value(
        VarIntCoder.of());

    @ProcessElement
    public void statefulProcessing(
        @Element KV<Long, Instant> currentElement,
        @StateId("ordered_list_state") OrderedListState<String> orderedListState,
        @StateId("initial_instant") ValueState<Instant> initialInstantState,
        @StateId("element_count") ValueState<Integer> elementCountState,
        OutputReceiver<String> outputReceiver) {

      Instant initialInstant = initialInstantState.read();
      int elementCount;

      Instant currentInstant = currentElement.getValue();

      if (initialInstant == null) {
        initialInstantState.write(currentInstant);
        elementCount = 0;
      } else {
        elementCount = elementCountState.read();
      }
      elementCountState.write(++elementCount);

      orderedListState.add(TimestampedValue.of(currentInstant.toString(), currentInstant));

      LOG.info("Received element #" + elementCount + ": " + currentInstant);

      if (elementCount % 11 == 0) {
        //--- Clear everything up to the current element
        orderedListState.clearRange(initialInstant, currentInstant);
        LOG.info("Cleared range from " + initialInstant + " upto " + currentInstant);
      }

      if (elementCount % 13 == 0) {
        Instant eventInTheClearedRange = eventTwoSecondsOlderThanInitial(initialInstant);
        LOG.info("Inserting an entry " + eventInTheClearedRange);
      }

      if (elementCount % 17 == 0) {
        LOG.info(
            "Trying to read OrderedStateList from " + initialInstant + " to " + currentInstant);
        //--- Attempt to read the data range form the initial instance
        Iterable<TimestampedValue<String>> bufferedElements = orderedListState.readRange(
            initialInstant, currentInstant);
        Iterator<TimestampedValue<String>> iterator = bufferedElements.iterator();
        if (!iterator.hasNext()) {
          String message = "No elements where found";
          LOG.error(message);
          outputReceiver.output(message);
        } else {
          TimestampedValue<String> firstElement = iterator.next();
          Instant eventInTheClearedRange = eventTwoSecondsOlderThanInitial(initialInstant);
          if (!firstElement.getTimestamp().equals(eventInTheClearedRange)) {
            String message =
                "Expected " + eventInTheClearedRange + " but got " + firstElement.getTimestamp();
            LOG.error(message);
            outputReceiver.output(message);
          }
        }
      }
    }

    private static Instant eventTwoSecondsOlderThanInitial(Instant initialInstant) {
      return initialInstant.plus(Duration.standardSeconds(2));
    }
  }

  @Test
  public void reproduceOrderedListStateBug() {
    PCollection<String> output = p
        .apply("Create streaming input",
            PeriodicImpulse.create().withInterval(Duration.standardSeconds(1)))
        .apply("to KV", ParDo.of(new MapInstantToKV()))
        .apply("Stateful ParDo", ParDo.of(new StatefulParDo()));

    output = output.apply("Into FixedWindows",
        Window.into(FixedWindows.of(Duration.standardMinutes(1))));
    //-- If PAssert fails, TestDataflowRunner will cancel the pipeline.
    PAssert.that(output).empty();

    p.runWithAdditionalOptionArgs(Arrays.asList("--runner=TestDataflowRunner",
        "--tempRoot=.", "--gcpTempLocation=gs://" + getTempGCSBucketName() + "/temp",
        "--usePublicIps=False",
        "--streaming", "--enableStreamingEngine"));
  }

  private static String getTempGCSBucketName() {
    String result = System.getenv(DATAFLOW_TEMP_BUCKET);
    if (result == null) {
      throw new IllegalStateException(
          "Running tests using Dataflow Runner requires a GCS bucket to stage test data. "
              + "Set up system property '" + DATAFLOW_TEMP_BUCKET
              + "' to provide the bucket name to the tests");
    }
    return result;
  }

}
