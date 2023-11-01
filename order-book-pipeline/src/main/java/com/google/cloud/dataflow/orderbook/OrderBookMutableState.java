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
import java.util.Map;
import org.apache.beam.sdk.extensions.ordered.MutableState;

public class OrderBookMutableState implements MutableState<OrderBookEvent, MarketDepth> {

  public int getDepth() {
    return depth;
  }

  public boolean isWithTrade() {
    return withTrade;
  }

  private int depth;
  private boolean withTrade;

  private OrderBookBuilder orderBookBuilder;

  OrderBookMutableState(int depth, boolean withTrade) {
    this.depth = depth;
    this.withTrade = withTrade;
    this.orderBookBuilder = new OrderBookBuilder();
  }

  OrderBookMutableState(int depth, boolean withTrade, Map<Long, Long> prices,
      OrderBookEvent lastEvent) {
    this.depth = depth;
    this.withTrade = withTrade;
    this.orderBookBuilder = new OrderBookBuilder(prices, lastEvent);
  }

  @Override
  public void mutate(OrderBookEvent event) {
    orderBookBuilder.processEvent(event);
  }

  @Override
  public MarketDepth produceResult() {
    return orderBookBuilder.getCurrentMarketDepth(depth, withTrade);
  }

  public Map<Long, Long> getPrices() {
    return orderBookBuilder.getPrices();
  }

  public OrderBookEvent getLastOrderBookEvent() {
    return orderBookBuilder.getLastOrderBookEvent();
  }
}
