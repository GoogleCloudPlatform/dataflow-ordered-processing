package com.google.cloud.simulator;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

/*
 * Simulator that generates orders against a matcher.
 */
public class Simulator {

  // Simple simulator -- return an iterable of OrderBookEvents
  static public Iterable<OrderBookEvent> getSimpleSimulator(int midprice, long genOrders, long seed) {
    QueuedProducer<OrderBookEvent> que = new QueuedProducer<OrderBookEvent>();
    new Simulator(que, 1, 100, genOrders, seed);
    return que;
  }

  // Complex simulator -- return an iterable of OrderBookEvents, across
  // multiple contracts
  static public Iterable<OrderBookEvent> getComplexSimulator(
      long startContract,
      long endContract,
      long midPrice,
      long genOrders,
      long seed) {

    QueuedProducer<OrderBookEvent> que = new QueuedProducer<OrderBookEvent>();

    // Start all of the simulators
    long ordersPerContract = genOrders / (endContract - startContract);
    for (long i = startContract; i < endContract; i++) {
      if (ordersPerContract > genOrders) {
        ordersPerContract = genOrders;
      }
      new Simulator(que, i, midPrice, ordersPerContract, seed);
      genOrders -= ordersPerContract;
    }

    return que;
  }

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

  final private QueuedProducer<OrderBookEvent> que;

  private long anchorMidprice;
  private long midprice;
  private long genOrders;
  Simulator(QueuedProducer<OrderBookEvent> que, long contractId, long midprice, long genOrders, long seed) {
    this.anchorMidprice = midprice;
    this.midprice = midprice;
    this.genOrders = genOrders;
    this.m = new Matcher(contractId);
    if (seed != 0) {
      this.r = new Random(seed);
    } else {
      this.r = new Random();
    }

    // Queue the first task
    this.que = que;
    this.que.add(0, new Callable<List<OrderBookEvent>>() {
      @Override
      public List<OrderBookEvent> call() throws Exception {
        return generateOrder();
      }
    });
  }

  private void addExecution(long price, long quantity) {
    if (quantity == 0)
      return;
    
    if (quantity > 0) {
      midprice = price;
      que.add(trailingTimeoutTicks, new Callable<List<OrderBookEvent>>() {
        @Override
        public List<OrderBookEvent> call() throws Exception {
          addExecution(price, -1 * quantity);
          return Arrays.asList();
        }
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

    // Adjust buy sell bias by how close we are to the outter edges of trading (+/- 50)
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
    final Order o = new Order(side, price, qty);
    
    // Decrement the generated orders
    this.genOrders -= 1;
    if (this.genOrders != 0) {
      que.add(1, new Callable<List<OrderBookEvent>>() {
        @Override
        public List<OrderBookEvent> call() throws Exception {
          return generateOrder();
        }
      });
    }

    // Remove the order in the future
    que.add(50, new Callable<List<OrderBookEvent>>() {
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
