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

import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.extensions.ordered.EventExaminer;

public class OrderBookEventExaminer implements
    EventExaminer<OrderBookEvent, OrderBookMutableState> {

  private final int depth;
  private final boolean withTrade;

  public OrderBookEventExaminer(int depth, boolean withTrade) {
    this.depth = depth;
    this.withTrade = withTrade;
  }

  @Override
  public boolean isInitialEvent(long sequenceNumber, OrderBookEvent event) {
    // We assume that all events in a session start with 1.
    return sequenceNumber == 1L;
  }

  @Override
  public OrderBookMutableState createStateOnInitialEvent(OrderBookEvent event) {
    OrderBookMutableState orderBookMutableState = new OrderBookMutableState(depth, withTrade);
    orderBookMutableState.mutate(event);
    return orderBookMutableState;
  }

  @Override
  public boolean isLastEvent(long sequenceNumber, OrderBookEvent event) {
    return event.getLastContractMessage();
  }
}
