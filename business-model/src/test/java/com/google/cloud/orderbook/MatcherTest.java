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
import java.util.HashMap;
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
  public void endTestMultiContract() {

    // Must be a multiple of 10 for number of events and number of orders
    int MAX_EVENTS = 20;

    // MAX_EVENTS must be dividable by CONTRACTS
    int CONTRACTS = 2;

    // Calculated number of orders to send per contract
    int NUM_ORDERS = MAX_EVENTS/CONTRACTS;

    // Expected last sequence numbers
    long LAST_CONTRACT_SEQ_ID = NUM_ORDERS+1;
    long LAST_SEQ_ID = (NUM_ORDERS + 1) * CONTRACTS + 1;


    // Create the matchers and context
    MatcherContext context = new MatcherContext(10, startTime, MAX_EVENTS/10);
    ArrayList<Matcher> matchers = new ArrayList<Matcher>();
    for (int i = 0; i < CONTRACTS; i++) {
      matchers.add(new Matcher(context, i));
    }

    // Add a bunch of orders to across all the contracts to execute.
    for (int i = 0; i < NUM_ORDERS; i++) {
      for (int j = 0; j < CONTRACTS; j++) {
        final Matcher m = matchers.get(j);
        context.add(i, () -> m.add(context.newOrder(OrderBookEvent.Side.BUY, 10, 10))); 
      }
    }

    // Track contract IDs
    HashMap<Long, Long> contractSeqId = new HashMap<Long, Long>();
    int seqId = 0;
    for (List<OrderBookEvent> obeList : context) {
      for (OrderBookEvent obe : obeList) {

        // Check sequence ID
        seqId ++;
        Assert.assertEquals("expected seqId", seqId, obe.getSeqId());
        if (seqId < LAST_SEQ_ID) {
          Assert.assertEquals("expected final message flag", false, obe.getLastMessage());
        } else if (seqId == LAST_SEQ_ID) {
          Assert.assertEquals("expected final message flag", true, obe.getLastMessage());
        } else {
          Assert.assertTrue("too many messages!", false);
        }

        // Check contract sequence ID (if non-zero)
        if (obe.getContractSeqId() > 0) {
          long nextContractSeqId = contractSeqId.compute(obe.getContractId(), (k, v) -> {
            if (v == null) {
              return 1L;
            } else {
              return v + 1;
            }
          });
          Assert.assertEquals("expected contractSeqId", nextContractSeqId, obe.getContractSeqId());

          if (nextContractSeqId < LAST_CONTRACT_SEQ_ID) {
            Assert.assertEquals("expected final message flag", false, obe.getLastContractMessage());
          } else if (nextContractSeqId == LAST_CONTRACT_SEQ_ID) {
            Assert.assertEquals("expected final message flag", true, obe.getLastContractMessage());
          } else {
            Assert.assertTrue("too many messages!", false);
          }
        }
      }
    }
  }

  @Test
  public void endTestSimple() {
    int MAX_EVENTS = 10;
    MatcherContext context = new MatcherContext(10, startTime, MAX_EVENTS/10);
    Matcher m = new Matcher(context, 1);

    // Add a bunch of orders to execute.
    for (int i = 0; i < 10; i++) {
      context.add(i, () -> m.add(context.newOrder(OrderBookEvent.Side.BUY, 100, 100))); 
    }

    int msgCount = 0;
    for (List<OrderBookEvent> obeList : context) {
      for (OrderBookEvent obe : obeList) {
        msgCount ++;
        if (msgCount < MAX_EVENTS+1) {
          Assert.assertEquals("expected new orders", obe.getType(), OrderBookEvent.Type.NEW);
          Assert.assertEquals("expected size of 1", msgCount, obe.getSeqId());
          Assert.assertEquals("expected size of 1", msgCount, obe.getContractSeqId());
        } else if (msgCount == MAX_EVENTS+1) {
          Assert.assertEquals("expected final contract order", true, obe.getLastContractMessage());
          Assert.assertEquals("expected final contract order", msgCount, obe.getContractSeqId());
          Assert.assertEquals("expected final contract order", msgCount, obe.getSeqId());
        } else if (msgCount == MAX_EVENTS+2) {
          Assert.assertEquals("expected final order", true, obe.getLastMessage());
          Assert.assertEquals("expected final contract order", msgCount, obe.getSeqId());
        } else {
          Assert.assertTrue("too many order events!", false);
        }
      }
    }
  }

  @Test
  public void matchTest() {
    MatcherContext context = new MatcherContext(10, startTime, 0);
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
    MatcherContext context = new MatcherContext(1000, startTime, 0);
    Matcher m = new Matcher(context, 1);

    // Add new sell order of q:100, p:100
    addOrder(m,
        context.newOrder(OrderBookEvent.Side.SELL, 100, 100),
        OrderBookEvent.newBuilder()
            .setTimestampMS(startTime)
            .setSeqId(1)
            .setContractSeqId(1)
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
            .setSeqId(2)
            .setContractSeqId(2)
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