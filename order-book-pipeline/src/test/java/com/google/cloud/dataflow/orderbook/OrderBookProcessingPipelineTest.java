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

package com.google.cloud.dataflow.orderbook;

import com.google.cloud.orderbook.OrderBookBuilder;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.orderbook.model.OrderBookEvent.Side;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingStatus;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;


public class OrderBookProcessingPipelineTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testSingleContractBatchProcessing() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(1L).setContractId(contractId).setContractSeqId(1)
            .setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(2L).setContractId(contractId).setContractSeqId(2)
            .setQuantity(20).setPrice(201).setSide(Side.BUY).build());

    OrderBookBuilder orderBookBuilder = new OrderBookBuilder();
    Collection<KV<SessionContractKey, MarketDepth>> expectedOutput = new ArrayList<>(
        inputEvents.size());

    long elementCount = 0;
    for (OrderBookEvent event : inputEvents) {
      elementCount++;
      orderBookBuilder.processEvent(event);

      expectedOutput.add(
          KV.of(SessionContractKey.create(
                  sessionId, event.getContractId()),
              orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));
    }

    PCollection<OrderBookEvent> events = p.apply("Input", Create.of(inputEvents));

    OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> orderedProcessingResult = events.apply(
        "Process in order", new OrderBookProducer(depth, withTrade));

    PCollection<KV<SessionContractKey, MarketDepth>> marketDepthResults = orderedProcessingResult.output();
    PAssert.that(marketDepthResults).containsInAnyOrder(expectedOutput);

    p.run();
  }

  @Test
  public void testSingleContractStreamingProcessingInPerfectOrder() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(1L).setContractId(contractId).setContractSeqId(1)
            .setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(2L).setContractId(contractId).setContractSeqId(2)
            .setQuantity(20).setPrice(201).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(2L).setContractId(contractId).setContractSeqId(3)
            .setQuantity(20).setPrice(201).setSide(Side.SELL).build()

    );

    OrderBookBuilder orderBookBuilder = new OrderBookBuilder();
    Collection<KV<SessionContractKey, MarketDepth>> expectedOutput = new ArrayList<>(
        inputEvents.size());
    Collection<KV<SessionContractKey, OrderedProcessingStatus>> expectedProcessingStatuses = new ArrayList<>(
        inputEvents.size());

    long elementCount = 0;
    for (OrderBookEvent event : inputEvents) {
      elementCount++;
      orderBookBuilder.processEvent(event);

      expectedOutput.add(
          KV.of(SessionContractKey.create(sessionId, event.getContractId()),
              orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));

      expectedProcessingStatuses.add(
          KV.of(SessionContractKey.create(sessionId, event.getContractId()),
              OrderedProcessingStatus.create(elementCount, 0, null, null, elementCount,
                  elementCount, 0,
                  false)));
    }

    testStreamingProcessing(depth, withTrade, inputEvents, expectedOutput,
        expectedProcessingStatuses);
  }

  @Test
  public void testSingleContractStreamingProcessingInReverseOrder() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(1L).setContractId(contractId).setContractSeqId(3)
            .setQuantity(20).setPrice(200).setSide(Side.SELL).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(1L).setContractId(contractId).setContractSeqId(2)
            .setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId)
            .setOrderId(2L).setContractId(contractId).setContractSeqId(1)
            .setQuantity(20).setPrice(201).setSide(Side.BUY).build());

    OrderBookBuilder orderBookBuilder = new OrderBookBuilder();
    Collection<KV<SessionContractKey, MarketDepth>> expectedOutput = new ArrayList<>(
        inputEvents.size());
    Collection<KV<SessionContractKey, OrderedProcessingStatus>> expectedProcessingStatuses = new ArrayList<>(
        inputEvents.size());

    List<OrderBookEvent> sortedEvents = new ArrayList<>(inputEvents);
    sortedEvents.sort(Comparator.comparingLong(OrderBookEvent::getContractSeqId));

    long elementCount = 0;

    for (OrderBookEvent event : sortedEvents) {
      elementCount++;
      orderBookBuilder.processEvent(event);
      expectedOutput.add(
          KV.of(SessionContractKey.create(sessionId, event.getContractId()),
              orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));
    }

    elementCount = 0;

    ++elementCount;
    SessionContractKey key = SessionContractKey.create(sessionId, contractId);
    expectedProcessingStatuses.add(
        KV.of(key, OrderedProcessingStatus.create(null, 1, 3L, 3L, elementCount, 0l, 0, false)));

    ++elementCount;
    expectedProcessingStatuses.add(
        KV.of(key, OrderedProcessingStatus.create(null, 2, 2L, 3L, elementCount, 0L, 0, false)));

    ++elementCount;
    expectedProcessingStatuses.add(
        KV.of(key, OrderedProcessingStatus.create(3L, 0, null, null, elementCount, 3L, 0, false)));

    testStreamingProcessing(depth, withTrade, inputEvents, expectedOutput,
        expectedProcessingStatuses);
  }

  private void testStreamingProcessing(int depth, boolean withTrade,
      List<OrderBookEvent> inputEvents,
      Collection<KV<SessionContractKey, MarketDepth>> expectedOutput,
      Collection<KV<SessionContractKey, OrderedProcessingStatus>> expectedProcessingStatuses) {
    Coder<OrderBookEvent> eventCoder = ProtoCoder.of(OrderBookEvent.class);

    // Simulate streaming data arriving with some delays.
    Instant now = Instant.now().minus(Duration.standardMinutes(20));
    TestStream.Builder<OrderBookEvent> messageFlow = TestStream.create(eventCoder)
        .advanceWatermarkTo(now);

    int delayInMilliseconds = 0;
    for (OrderBookEvent e : inputEvents) {
      messageFlow = messageFlow.advanceWatermarkTo(now.plus(Duration.millis(++delayInMilliseconds)))
          .addElements(e);
    }

    // Needed to force the processing time based timers.
    messageFlow = messageFlow.advanceProcessingTime(Duration.standardMinutes(15));

    PCollection<OrderBookEvent> events = p.apply("Input", messageFlow.advanceWatermarkToInfinity());

    OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> orderedProcessingResult = events.apply(
        "Process in order",
        new OrderBookProducer(depth, withTrade).produceStatusUpdatesOnEveryEvent()
            .produceStatusUpdatesInSeconds(-1));

    PCollection<KV<SessionContractKey, OrderedProcessingStatus>> processingStatuses = orderedProcessingResult.processingStatuses();
    PAssert.that(processingStatuses).containsInAnyOrder(expectedProcessingStatuses);

    PCollection<KV<SessionContractKey, MarketDepth>> marketDepthResults = orderedProcessingResult.output();
    PAssert.that(marketDepthResults).containsInAnyOrder(expectedOutput);

    p.run();
  }

}
