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

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.MarketDepth.PriceQuantity;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Arrays;
import java.util.GregorianCalendar;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests of OrderBookBuilder.
 */
@RunWith(JUnit4.class)
public class OrderBookBuilderTest {

  // 2023-01-01 noon in milliseconds.
  final static long startTime = (new GregorianCalendar(2023, 0, 1, 12, 0, 0)).getTimeInMillis();

  void add(OrderBookBuilder builder, Matcher m, Order... orders) {
    for (Order o : orders) {
      for (OrderBookEvent obe : m.add(o)) {
        builder.processEvent(obe);
      }
    }
  }

  @Test
  public void simpleTest() {
    OrderBookBuilder builder = new OrderBookBuilder();
    MatcherContext context = new MatcherContext(1000, startTime, 0);
    Matcher m = new Matcher(context, 1);

    // Add a series of orders.
    add(builder, m, context.newOrder(OrderBookEvent.Side.BUY, 100, 100));
    add(builder, m, context.newOrder(OrderBookEvent.Side.SELL, 101, 100));

    MarketDepth d = builder.getCurrentMarketDepth(10, false);
    Assert.assertEquals("expected depth to match", d,
        MarketDepth.newBuilder()
            .setTimestampMS(startTime)
            .setContractId(1)
            .setContractSeqId(2)
            .setSeqId(2)
            .addAllBids(Arrays.asList(
                PriceQuantity.newBuilder().setPrice(100).setQuantity(100).build()
            ))
            .addAllOffers(Arrays.asList(
                PriceQuantity.newBuilder().setPrice(101).setQuantity(100).build()
            ))
            .build());
  }
}