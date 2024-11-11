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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingStatus;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrderBookProcessingPipelineTest {

  private static final boolean SEQUENCING_PER_KEY = true;
  private static final boolean GLOBAL_SEQUENCING = !SEQUENCING_PER_KEY;
  private static final Collection<KV<SessionContractKey, OrderedProcessingStatus>> DONT_CHECK_PROCESSING_STATUSES = null;
  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testSingleContractBatchPerSequenceProcessing() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L).setContractId(contractId)
            .setContractSeqId(1).setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(2L).setContractId(contractId)
            .setContractSeqId(2).setQuantity(20).setPrice(201).setSide(Side.BUY).build());

    OrderBookBuilder orderBookBuilder = new OrderBookBuilder();
    Collection<KV<SessionContractKey, MarketDepth>> expectedOutput = new ArrayList<>(
        inputEvents.size());

    for (OrderBookEvent event : inputEvents) {
      orderBookBuilder.processEvent(event);

      expectedOutput.add(KV.of(SessionContractKey.create(sessionId, event.getContractId()),
          orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));
    }

    PCollection<OrderBookEvent> events = p.apply("Input", Create.of(inputEvents));

    OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> orderedProcessingResult = events.apply(
        "Process in order", new OrderBookProducer(depth, withTrade, 10000, SEQUENCING_PER_KEY));

    PCollection<KV<SessionContractKey, MarketDepth>> marketDepthResults = orderedProcessingResult.output();
    PAssert.that(marketDepthResults).containsInAnyOrder(expectedOutput);

    p.run();
  }

  @Test
  public void testSingleContractStreamingPerSequenceProcessingInPerfectOrder() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L).setContractId(contractId)
            .setContractSeqId(1).setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(2L).setContractId(contractId)
            .setContractSeqId(2).setQuantity(20).setPrice(201).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(2L).setContractId(contractId)
            .setContractSeqId(3).setQuantity(20).setPrice(201).setSide(Side.SELL).build()

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

      expectedOutput.add(KV.of(SessionContractKey.create(sessionId, event.getContractId()),
          orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));

      expectedProcessingStatuses.add(
          KV.of(SessionContractKey.create(sessionId, event.getContractId()),
              OrderedProcessingStatus.create(elementCount, 0, null, null, elementCount,
                  elementCount, 0, false)));
    }

    testStreamingProcessing(depth, withTrade, inputEvents, expectedOutput,
        expectedProcessingStatuses, SEQUENCING_PER_KEY);
  }

  @Test
  public void testMultipleContractStreamingGlobalSequenceProcessingInReverseOrder() {
    int depth = 1;
    boolean withTrade = true;
    long contractId1 = 345L;
    long contractId2 = 678L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L)
            .setContractId(contractId1).setContractSeqId(3).setQuantity(20).setPrice(200)
            .setSide(Side.SELL).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L)
            .setContractId(contractId1).setContractSeqId(2).setQuantity(10).setPrice(200)
            .setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(2L)
            .setContractId(contractId2).setContractSeqId(1).setQuantity(20).setPrice(201)
            .setSide(Side.BUY).build());

    // Map is by the contract id - separate builder for each contract.
    Map<Long, OrderBookBuilder> orderBookBuilders = new HashMap<>();
    orderBookBuilders.put(contractId1, new OrderBookBuilder());
    orderBookBuilders.put(contractId2, new OrderBookBuilder());

    Collection<KV<SessionContractKey, MarketDepth>> expectedOutput = new ArrayList<>(
        inputEvents.size());

    List<OrderBookEvent> sortedEvents = new ArrayList<>(inputEvents);
    sortedEvents.sort(Comparator.comparingLong(OrderBookEvent::getContractSeqId));

    for (OrderBookEvent event : sortedEvents) {
      OrderBookBuilder orderBookBuilder = orderBookBuilders.get(event.getContractId());
      orderBookBuilder.processEvent(event);
      expectedOutput.add(KV.of(SessionContractKey.create(sessionId, event.getContractId()),
          orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));
    }

    testStreamingProcessing(depth, withTrade, inputEvents, expectedOutput,
        DONT_CHECK_PROCESSING_STATUSES, GLOBAL_SEQUENCING);
  }

  @Test
  public void testSingleContractStreamingPerSequenceProcessingInReverseOrder() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;
    String sessionId = "session-id-1";

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L).setContractId(contractId)
            .setContractSeqId(3).setQuantity(20).setPrice(200).setSide(Side.SELL).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(1L).setContractId(contractId)
            .setContractSeqId(2).setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setSessionId(sessionId).setOrderId(2L).setContractId(contractId)
            .setContractSeqId(1).setQuantity(20).setPrice(201).setSide(Side.BUY).build());

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
      expectedOutput.add(KV.of(SessionContractKey.create(sessionId, event.getContractId()),
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
        expectedProcessingStatuses, SEQUENCING_PER_KEY);
  }

  private void testStreamingProcessing(int depth, boolean withTrade,
      List<OrderBookEvent> inputEvents,
      Collection<KV<SessionContractKey, MarketDepth>> expectedOutput,
      Collection<KV<SessionContractKey, OrderedProcessingStatus>> expectedProcessingStatuses,
      boolean sequencingPerKey) {
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

    int maxElementsPerBundle = 10000;
    OrderBookProducer orderBookProducer = new OrderBookProducer(depth, withTrade,
        maxElementsPerBundle, sequencingPerKey);
    if (sequencingPerKey) {
      orderBookProducer
          .produceStatusUpdatesOnEveryEvent()
          .withStatusUpdateFrequency(null);
    } else {
      orderBookProducer.withStatusUpdateFrequency(Duration.standardMinutes(3));
    }
    OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> orderedProcessingResult = events.apply(
        "Process in order", orderBookProducer);

    PCollection<KV<SessionContractKey, OrderedProcessingStatus>> processingStatuses = orderedProcessingResult.processingStatuses();
    if (sequencingPerKey) {
      PAssert.that(processingStatuses).containsInAnyOrder(expectedProcessingStatuses);
    } else {
      // We don't get the statuses predictably in global sequence processing. We just print them
      // to help with debugging
      processingStatuses.apply("Print processing status", ParDo.of(new PrintProcessingStatus()));
    }

    PCollection<KV<SessionContractKey, MarketDepth>> marketDepthResults = orderedProcessingResult.output();
    PAssert.that(marketDepthResults).containsInAnyOrder(expectedOutput);

    p.run();
  }

  private static class PrintProcessingStatus extends
      DoFn<KV<SessionContractKey, OrderedProcessingStatus>, Boolean> {

    Logger LOG = LoggerFactory.getLogger(PrintProcessingStatus.class);

    @ProcessElement
    public void process(@Element KV<SessionContractKey, OrderedProcessingStatus> status) {
      LOG.info("Status: " + status);
    }
  }
}
