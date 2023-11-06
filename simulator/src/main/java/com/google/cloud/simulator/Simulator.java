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

package com.google.cloud.simulator;

import com.google.cloud.orderbook.Matcher;
import com.google.cloud.orderbook.MatcherContext;
import com.google.cloud.orderbook.Order;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/*
 * Simulator that generates orders against a matcher.
 */
public class Simulator {

  /**
   * Create a simple simulator for one contract
   * 
   * @param midprice   Starting midprice (long, e.g., 100)
   * @param genOrders  How many orders to generate and cancel in the simulator (0 = unlimited)
   * @param seed       Random seed (0 = default randomization).
   *                   This allows for deterministic behaviour if needed.
   * 
   * @return Iterable<OrderBookEvent> -- produce OrderBookEvents from the simulator
   */
  static public Iterator<List<OrderBookEvent>> getSimpleSimulator(MatcherContext context, int midprice, long genOrders, long seed) {
    new Simulator(context, 1, 100, genOrders, seed);

    context.addAtShutdown(new Callable<List<OrderBookEvent>>() {
      @Override
      public List<OrderBookEvent> call() throws Exception {
        return Arrays.asList(context.buildFinalOrderBookEvent().build());
      }
    });

    return context.iterator();
  }

  /**
   * Create a complex (multiple contract) simulator.
   * 
   * @param startContract  Starting contract ID
   * @param endContract    End contract ID (exclusive)
   * @param midPrice       Starting mid price for all contracts
   * @param genOrders      How many orders to generate in total (0 = unlimited)
   * @param seed           Random seed (0 = default randomization)
   * 
   * @return Iterable<OrderbookEvent> -- produce OrderBookEvents from the simulator
   */
  static public Iterator<List<OrderBookEvent>> getComplexSimulator(
      MatcherContext context,
      long numContracts,
      long midPrice,
      long genOrders,
      long seed) {

    // Start all of the simulators
    for (long i = 1; i <= numContracts; i++) {
      new Simulator(context, i, midPrice, genOrders, seed);
    }

    context.addAtShutdown(new Callable<List<OrderBookEvent>>() {
      @Override
      public List<OrderBookEvent> call() throws Exception {
        return Arrays.asList(context.buildFinalOrderBookEvent().build());
      }
    });

    return context.iterator();
  }

  private final MatcherContext context;
  private final Matcher m;
  private final Random r;

  private double buySellBias = 0.5;

  // Min/max quantity for orders (randomized this range)
  private long minQty = 10;
  private long maxQty = 100;

  // Range of price (low to high) around midpoint
  private double range = 10.0;
  private double shift = 3.0;
  private long trailingShares = 0;
  private double trailingSV = 0.0;
  private long trailingTimeoutTicks = 50;

  private long anchorMidprice;
  private long midprice;
  private long genOrders;

  private Simulator(MatcherContext context, long contractId, long midprice, long genOrders, long seed) {
    this.anchorMidprice = midprice;
    this.midprice = midprice;
    this.genOrders = genOrders;
    this.m = new Matcher(context, contractId);
    if (seed != 0) {
      this.r = new Random(seed);
    } else {
      this.r = new Random();
    }

    // Queue the first task
    this.context = context;
    this.context.add(0, () -> generateOrder());

    // Queue the last task at shutdown
    this.context.addAtShutdown(() -> m.shutdown());
  }

  private void addExecution(long price, long quantity) {
    if (quantity == 0)
      return;
    
    if (quantity > 0) {
      midprice = price;
      context.add(trailingTimeoutTicks, () -> {
        addExecution(price, -1 * quantity);
        return Arrays.asList();
      });
    }

    trailingShares += quantity;
    trailingSV += quantity * price;
  }

  private List<OrderBookEvent> generateOrder() {
    long qty = (long)(minQty + (maxQty - minQty) * r.nextDouble());
 
    // Set back to 0.02
    if (r.nextDouble() < 0.0) {
      buySellBias = r.nextDouble();
      if (buySellBias > 0.65)
        buySellBias = 0.65;
      else if (buySellBias < 0.35)
        buySellBias = 0.35;
    }

    // Adjust midprice to trailing average traded price
    if (trailingShares > 0)
      midprice = Math.round(trailingSV / trailingShares);

    // Adjust buy sell bias by how close we are to the outer edges of trading (+/- 50)
    double priceShift = (r.nextDouble() * range) - (range / 2.0);
    if (midprice < anchorMidprice) {
      priceShift += Math.pow((anchorMidprice - midprice)/50, 2) * r.nextDouble() * 3;
    } else {
      priceShift -= Math.pow((midprice - anchorMidprice)/50, 2) * r.nextDouble() * 3;
    }
    
    long price;
    OrderBookEvent.Side side;
    if (r.nextDouble() < buySellBias) {
      side = OrderBookEvent.Side.BUY;
      price = Math.round(midprice + (priceShift - shift));
    } else {
      side = OrderBookEvent.Side.SELL;
      price = Math.round(midprice + (priceShift + shift));
    }

    // Determine the Order
    final Order o = context.newOrder(side, price, qty);
    
    // Decrement the generated orders
    this.genOrders -= 1;
    if (this.genOrders != 0) {
      context.add(1, new Callable<List<OrderBookEvent>>() {
        @Override
        public List<OrderBookEvent> call() throws Exception {
          return generateOrder();
        }
      });
    }

    // Remove the order in the future
    context.add(50, new Callable<List<OrderBookEvent>>() {
      @Override
      public List<OrderBookEvent> call() throws Exception {
        return m.remove(o);
      }
    });

    // Add the order
    List<OrderBookEvent> b = m.add(o);

    // Adjust the fills based on execution events
    for (OrderBookEvent obe : b) {
      if (obe.getQuantityFilled() > 0) {
        addExecution(obe.getPrice(), obe.getQuantityFilled());
      }
    }

    // Return the events
    return b;
  }
}
