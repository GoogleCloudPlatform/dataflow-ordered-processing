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
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessor;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingStatus;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;


public class OrderBookProcessingPipelineTest {

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testBatchProcessing() {
    int depth = 1;
    boolean withTrade = true;
    long contractId = 345L;

    List<OrderBookEvent> inputEvents = Arrays.asList(
        OrderBookEvent.newBuilder().setOrderId(1L).setContractId(contractId)
            .setContractSeqId(1).setQuantity(10).setPrice(200).setSide(Side.BUY).build(),

        OrderBookEvent.newBuilder().setOrderId(2L).setContractId(contractId)
            .setContractSeqId(2).setQuantity(20).setPrice(201).setSide(Side.BUY).build());

    OrderBookBuilder orderBookBuilder = new OrderBookBuilder();
    Collection<KV<Long, MarketDepth>> expectedOutput = new ArrayList<>(inputEvents.size());
    Collection<KV<Long, OrderedProcessingStatus>> expectedProcessingStatuses = new ArrayList<>(
        inputEvents.size());

    long elementCount = 0;
    for (OrderBookEvent event : inputEvents) {
      elementCount++;
      orderBookBuilder.processEvent(event);
      expectedOutput.add(
          KV.of(event.getContractId(), orderBookBuilder.getCurrentMarketDepth(depth, withTrade)));

      expectedProcessingStatuses.add(KV.of(event.getContractId(),
          OrderedProcessingStatus.create(elementCount, 0, null, null, elementCount)));
    }

    Coder<OrderBookEvent> eventCoder = ProtoCoder.of(OrderBookEvent.class);
    Coder<OrderBookMutableState> stateCoder = OrderBookCoder.of();
    Coder<Long> keyCoder = VarLongCoder.of();
    Coder<MarketDepth> marketDepthCoder = ProtoCoder.of(MarketDepth.class);

    PCollection<KV<Long, KV<Long, OrderBookEvent>>> events = p.apply("Input",
            Create.of(inputEvents))
        .apply("Convert to KV", ParDo.of(new ConvertOrderToKV()));

    OrderedEventProcessor<OrderBookEvent, Long, MarketDepth, OrderBookMutableState> orderedProcessor =
        OrderedEventProcessor.create(new InitialStateCreator(depth, withTrade), eventCoder,
                stateCoder,
                keyCoder, marketDepthCoder)
            .withInitialSequence(1L)
            .produceStatusUpdatesOnEveryEvent(true);
    OrderedEventProcessorResult<Long, MarketDepth> orderedProcessingResult = events.apply(
        "Process in order", orderedProcessor);

    PCollection<KV<Long, OrderedProcessingStatus>> processingStatuses = orderedProcessingResult.processingStatuses();
    PAssert.that(processingStatuses).containsInAnyOrder(expectedProcessingStatuses);

    PCollection<KV<Long, MarketDepth>> marketDepthResults = orderedProcessingResult.output();
    PAssert.that(marketDepthResults).containsInAnyOrder(expectedOutput);

    p.run();
  }

}
