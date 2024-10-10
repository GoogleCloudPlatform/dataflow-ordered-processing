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

import com.google.cloud.dataflow.orderbook.SessionContractKey.SessionContractKeyCoder;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessor;
import org.apache.beam.sdk.extensions.ordered.OrderedEventProcessorResult;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingHandler;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Transform to encapsulate the details of changing the source PCollection to the shape required by
 * the OrderedEventProcessor and the configuration of that transform.
 */

public class OrderBookProducer extends
    PTransform<PCollection<OrderBookEvent>, OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent>> {

  private final int depth;
  private final boolean withTrade;

  private final boolean sequencingPerKey;

  private boolean produceStatusUpdatesOnEveryEvent = false;
  private Duration statusUpdateFrequency = null;

  private final int maxElementsPerBundle;

  public OrderBookProducer(int depth, boolean withTrade, int maxElementsPerBundle,
      boolean sequencingPerKey) {
    this.depth = depth;
    this.withTrade = withTrade;
    this.maxElementsPerBundle = maxElementsPerBundle;
    this.sequencingPerKey = sequencingPerKey;
  }

  public OrderBookProducer produceStatusUpdatesOnEveryEvent() {
    this.produceStatusUpdatesOnEveryEvent = true;
    return this;
  }

  public OrderBookProducer withStatusUpdateFrequency(Duration duration) {
    this.statusUpdateFrequency = duration;
    return this;
  }

  @Override
  public OrderedEventProcessorResult<SessionContractKey, MarketDepth, OrderBookEvent> expand(
      PCollection<OrderBookEvent> input) {
    input.getPipeline().getCoderRegistry()
        .registerCoderForClass(SessionContractKey.class, SessionContractKeyCoder.of());

    OrderedProcessingHandler<OrderBookEvent, SessionContractKey, OrderBookMutableState, MarketDepth> handler;
    if (sequencingPerKey) {
      handler = new OrderBookOrderedProcessingHandler(depth, withTrade);
      handler.setProduceStatusUpdateOnEveryEvent(produceStatusUpdatesOnEveryEvent);
      handler.setMaxOutputElementsPerBundle(maxElementsPerBundle);
      handler.setStatusUpdateFrequency(statusUpdateFrequency);
    } else {
      handler = new OrderBookOrderedProcessingGlobalSequenceHandler(depth, withTrade);
      handler.setProduceStatusUpdateOnEveryEvent(false);
      handler.setMaxOutputElementsPerBundle(maxElementsPerBundle);
      handler.setStatusUpdateFrequency(statusUpdateFrequency);
    }

    OrderedEventProcessor<OrderBookEvent, SessionContractKey, MarketDepth, OrderBookMutableState> orderedProcessor =
        OrderedEventProcessor.create(handler);

    return input
        .apply("Convert to KV", ParDo.of(new ConvertOrderBookEventToKV()))
        .apply("Produce Market Depths", orderedProcessor);
  }

}
