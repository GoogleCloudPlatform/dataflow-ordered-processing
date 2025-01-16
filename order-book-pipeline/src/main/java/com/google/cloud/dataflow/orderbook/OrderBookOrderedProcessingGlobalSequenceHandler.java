/*
 * Copyright 2024 Google LLC
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

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.extensions.ordered.EventExaminer;
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingHandler;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

/**
 * Handler use for global sequence processing.
 */
class OrderBookOrderedProcessingGlobalSequenceHandler extends
    OrderedProcessingHandler.OrderedProcessingGlobalSequenceHandler<OrderBookEvent, SessionContractKey, OrderBookMutableState, MarketDepth> {

  private final int depth;
  private final boolean withLastTrade;

  public OrderBookOrderedProcessingGlobalSequenceHandler(int depth, boolean withLastTrade) {
    super(OrderBookEvent.class, SessionContractKey.class, OrderBookMutableState.class,
        MarketDepth.class);
    this.depth = depth;
    this.withLastTrade = withLastTrade;
  }

  @Override
  public @NonNull EventExaminer<OrderBookEvent, OrderBookMutableState> getEventExaminer() {
    return new OrderBookEventExaminer(depth, withLastTrade);
  }

  @Override
  public @Nullable @UnknownKeyFor @Initialized Duration getStatusUpdateFrequency() {
//    return Duration.standardSeconds(5);
    return null;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized boolean isProduceStatusUpdateOnEveryEvent() {
    return false;
  }
}
