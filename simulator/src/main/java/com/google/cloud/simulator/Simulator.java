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

/*
 * Simulator that generates orders against a matcher.
 */
public class Simulator {

  /**
   * Create a complex (multiple contract) simulator.
   *
   * @param context           MatcherContext for the context of the matching engine
   * @param numContracts      Number of contracts to generate
   * @param midPrice          Starting mid price for all contracts
   * @param seed              Random seed (0 = default randomization)
   * @param useGlobalSequence Use global sequence number rather than per symbol sequence
   * @return Iterable<OrderbookEvent> -- produce OrderBookEvents from the simulator
   */
  static public Iterator<List<OrderBookEvent>> getComplexSimulator(
      MatcherContext context,
      long numContracts,
      long midPrice,
      long seed,
      long degreeDistribution,
      boolean useGlobalSequence) {

    // Minimum tick
    long minTicks = 10;

    // Maximum delay -- number of ticks to wait until sending the next order
    long maxTicks = 50;

    // Start all simulators
    for (long i = 1; i <= numContracts; i++) {

      // Calculate a number between 0 and 1 (but not one)
      double degreeToOne = 1 - Math.pow(((double) i / numContracts), degreeDistribution);

      // Convert to the number of ticks
      long orderTicks = minTicks + Math.round((maxTicks - minTicks) * degreeToOne);

      new Simulator(context, i, midPrice, seed, orderTicks);
    }

    return context.iterator();
  }

  private final MatcherContext context;
  private final Matcher matcher;
  private final Random random;

  private double buySellBias = 0.5;

  // Min/max quantity for orders (randomized this range)
  private long minQty = 10;
  private long maxQty = 100;

  // Range of price (low to high) around midpoint
  private double range = 10.0;
  private double shift = 3.0;
  private long trailingShares = 0;
  private double trailingSV = 0.0;

  // Configuration for frequency of orders
  final private long BOOK_SIZE = 20;
  final private long trailingTimeoutTicks;
  final private long cancelTicks;
  final private long orderTicks;

  private long anchorMidprice;
  private long midprice;

  private Simulator(MatcherContext context, long contractId, long midprice, long seed,
      long orderTicks) {
    this.anchorMidprice = midprice;
    this.midprice = midprice;
    this.matcher = new Matcher(context, contractId);
    if (seed != 0) {
      this.random = new Random(seed);
    } else {
      this.random = new Random();
    }

    // Calculate ticks
    this.orderTicks = orderTicks;
    this.cancelTicks = BOOK_SIZE * orderTicks;
    this.trailingTimeoutTicks = cancelTicks;

    // Queue the first task
    this.context = context;
    this.context.add(0, () -> generateOrder());
  }

  private void addExecution(long price, long quantity) {
    if (quantity == 0) {
      return;
    }

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
    long qty = (long) (minQty + (maxQty - minQty) * random.nextDouble());

    // Set back to 0.02
    if (random.nextDouble() < 0.0) {
      buySellBias = random.nextDouble();
      if (buySellBias > 0.65) {
        buySellBias = 0.65;
      } else if (buySellBias < 0.35) {
        buySellBias = 0.35;
      }
    }

    // Adjust midprice to trailing average traded price
    if (trailingShares > 0) {
      midprice = Math.round(trailingSV / trailingShares);
    }

    // Adjust buy sell bias by how close we are to the outer edges of trading (+/- 50)
    double priceShift = (random.nextDouble() * range) - (range / 2.0);
    if (midprice < anchorMidprice) {
      priceShift += Math.pow((anchorMidprice - midprice) / 50, 2) * random.nextDouble() * 3;
    } else {
      priceShift -= Math.pow((midprice - anchorMidprice) / 50, 2) * random.nextDouble() * 3;
    }

    long price;
    OrderBookEvent.Side side;
    if (random.nextDouble() < buySellBias) {
      side = OrderBookEvent.Side.BUY;
      price = Math.round(midprice + (priceShift - shift));
    } else {
      side = OrderBookEvent.Side.SELL;
      price = Math.round(midprice + (priceShift + shift));
    }

    // Determine the Order
    final Order o = context.newOrder(side, price, qty);

    context.add(orderTicks, () -> generateOrder());

    // Remove the order in the future
    context.add(orderTicks + cancelTicks, () -> matcher.remove(o));

    // Add the order
    List<OrderBookEvent> b = matcher.add(o);

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
