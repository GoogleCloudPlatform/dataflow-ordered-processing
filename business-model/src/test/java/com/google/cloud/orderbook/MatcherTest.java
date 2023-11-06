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

package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MatcherTest {

  // 2023-01-01 noon in milliseconds.
  final static long startTime = (new GregorianCalendar(2023, 0, 1, 12, 0, 0)).getTimeInMillis();

  private void expectMatches(List<OrderBookEvent> results, List<Long> expectedPrices,
      List<Long> expectedQty) {
    ArrayList<Long> prices = new ArrayList<Long>();
    ArrayList<Long> qty = new ArrayList<Long>();
    for (OrderBookEvent obe : results) {
      if (obe.getType().equals(OrderBookEvent.Type.EXECUTED)) {
        prices.add(obe.getPrice());
        qty.add(obe.getQuantityFilled());
      }
    }

    Assert.assertEquals("Expected prices to match", prices, expectedPrices);
    Assert.assertEquals("Expected qty to match", qty, expectedQty);
  }

  private void addOrder(Matcher m, Order order, OrderBookEvent... events) {
    Assert.assertEquals("Expected order events to match",
        m.add(order),
        Arrays.asList(events)
    );
  }

  @Test
  public void matchTest() {
    MatcherContext context = new MatcherContext(10, startTime);
    Matcher m = new Matcher(context, 1);

    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 100, 100)), Arrays.asList(),
        Arrays.asList());
    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 100, 100)), Arrays.asList(),
        Arrays.asList());
    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 100, 100)), Arrays.asList(),
        Arrays.asList());
    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 101, 100)), Arrays.asList(),
        Arrays.asList());
    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 101, 100)), Arrays.asList(),
        Arrays.asList());
    expectMatches(m.add(context.newOrder(OrderBookEvent.Side.SELL, 102, 100)), Arrays.asList(),
        Arrays.asList());

    expectMatches(
        m.add(context.newOrder(OrderBookEvent.Side.BUY, 100, 150)),
        Arrays.asList(100L, 100L),
        Arrays.asList(100L, 50L)
    );
  }

  @Test
  public void simpleTest() {
    MatcherContext context = new MatcherContext(1000, startTime);
    Matcher m = new Matcher(context, 1);

    // Add new sell order of q:100, p:100
    addOrder(m,
        context.newOrder(OrderBookEvent.Side.SELL, 100, 100),
        OrderBookEvent.newBuilder()
            .setTimestampMS(startTime)
            .setSeqId(0)
            .setContractSeqId(0)
            .setContractId(1)
            .setType(OrderBookEvent.Type.NEW)
            .setOrderId(1)
            .setSide(OrderBookEvent.Side.SELL)
            .setPrice(100)
            .setQuantity(100)
            .setQuantityRemaining(100)
            .setQuantityFilled(0)
            .setMatchNumber(0)
            .build()
    );

    // Add new buy order of q:100, p:100
    // See execution of original sell order

    addOrder(m,
        context.newOrder(OrderBookEvent.Side.BUY, 100, 100),
        OrderBookEvent.newBuilder()
            .setTimestampMS(startTime)
            .setSeqId(1)
            .setContractSeqId(1)
            .setContractId(1)
            .setType(OrderBookEvent.Type.EXECUTED)
            .setOrderId(1)
            .setSide(OrderBookEvent.Side.SELL)
            .setPrice(100)
            .setQuantity(100)
            .setQuantityRemaining(0)
            .setQuantityFilled(100)
            .setMatchNumber(0)
            .build()
    );
  }
}