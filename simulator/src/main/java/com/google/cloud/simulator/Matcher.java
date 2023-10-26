package com.google.cloud.simulator;

import static com.google.cloud.orderbook.model.OrderBookEvent.*;

import com.google.cloud.orderbook.model.OrderBookEvent;
import com.google.cloud.orderbook.model.OrderBookEvent.Builder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Matcher for a single contract that can produce OrderBookEvents
 *
 * There must be only one Matcher class per contractId, but there can
 * be in parallel multiple matchers for different contractIds.
 */
class Matcher {

  // Global Sequence ID -- atomic long to guarantee consistency
  static private AtomicLong nextSeqId = new AtomicLong(0);

  // Ordering orders in a tree -- ordered by price and orderId
  // (orderId is always increasing)
  private class OrderKey {

    OrderKey(long price, long orderId) {
      this.price = price;
      this.orderId = orderId;
    }

    long price;
    long orderId;

    public String toString() {
      return String.format("p=%d id=%d", price, orderId);
    }
  }

  final private long contractId;
  private long matchId = 0;
  private long seqId = 0;

  // Create a matcher for a contractId
  Matcher(long contractId) {
    this.contractId = contractId;
  }

  final private TreeMap<OrderKey, Order> bidOrderList = new TreeMap<OrderKey, Order>(
      new Comparator<OrderKey>() {
        @Override
        public int compare(OrderKey k0, OrderKey k1) {
          if (k0.price > k1.price) {
            return -1;
          }
          if (k0.price < k1.price) {
            return 1;
          }
          if (k0.orderId < k1.orderId) {
            return -1;
          }
          if (k0.orderId > k1.orderId) {
            return 1;
          }
          return 0;
        }
      }
  );
  final private TreeMap<OrderKey, Order> askOrderList = new TreeMap<OrderKey, Order>(
      new Comparator<OrderKey>() {
        @Override
        public int compare(OrderKey k0, OrderKey k1) {
          if (k0.price < k1.price) {
            return -1;
          }
          if (k0.price > k1.price) {
            return 1;
          }
          if (k0.orderId < k1.orderId) {
            return -1;
          }
          if (k0.orderId > k1.orderId) {
            return 1;
          }
          return 0;
        }
      }
  );

  List<OrderBookEvent> remove(Order o) {
    Order old = null;
    if (o.getSide() == Side.BUY) {
      old = bidOrderList.remove(new OrderKey(o.getPrice(), o.getOrderId()));
    } else {
      old = askOrderList.remove(new OrderKey(o.getPrice(), o.getOrderId()));
    }

    if (old != null) {
      return Arrays.asList(buildEvent(Type.DELETED, o).build());
    } else {
      return Arrays.asList();
    }
  }

  List<OrderBookEvent> add(Order o) {

    // Find match events
    ArrayList<OrderBookEvent> events = match(o);

    // Stop now if filled (IOC orders not used)
    if (o.getQuantityRemaining() == 0) {
      return events;
    }

    // Add order to book
    if (o.getSide() == Side.BUY) {
      bidOrderList.put(new OrderKey(o.getPrice(), o.getOrderId()), o);
    } else {
      askOrderList.put(new OrderKey(o.getPrice(), o.getOrderId()), o);
    }

    // Add new order event
    events.add(buildEvent(Type.NEW, o).build());

    return events;
  }

  private ArrayList<OrderBookEvent> match(Order o) {
    ArrayList<OrderBookEvent> matches = new ArrayList<>();

    // Get the right iterator to match against
    Iterator<Map.Entry<OrderKey, Order>> it;
    if (o.getSide() == Side.BUY) {
      it = askOrderList.entrySet().iterator();
    } else {
      it = bidOrderList.entrySet().iterator();
    }

    // Keep finding orders
    while (o.getQuantityRemaining() > 0 && it.hasNext()) {
      Map.Entry<OrderKey, Order> entry = it.next();

      // Stop if the price doesn't match
      if (o.getSide() == Side.BUY) {
        if (o.getPrice() < entry.getKey().price) {
          break;
        }
      } else {
        if (o.getPrice() > entry.getKey().price) {
          break;
        }
      }

      // Calculate the fill amount
      long fillQty = Math.min(o.getQuantityRemaining(),
          entry.getValue().getQuantityRemaining());

      // Reduce quantity on each order
      entry.getValue().fillOrder(fillQty);
      o.fillOrder(fillQty);

      // Create an execution event (passive side only)
      matches.add(buildEvent(Type.EXECUTED, entry.getValue())
          .setQuantityFilled(fillQty)
          .setMatchNumber(matchId)
          .build());

      matchId++;

      // Remove order if fully matched
      if (entry.getValue().getQuantityRemaining() == 0) {
        it.remove();
      }
    }

    return matches;
  }

  private Builder buildEvent(Type type, Order order) {
    Builder builder = newBuilder()
        .setTimestampMS(System.currentTimeMillis())
        .setSeqId(nextSeqId.getAndIncrement())
        .setContractSeqId(seqId++)
        .setContractId(contractId)
        .setType(type)
        .setOrderId(order.getOrderId())
        .setPrice(order.getPrice())
        .setSide(order.getSide())
        .setQuantity(order.getQuantity())
        .setQuantityRemaining(order.getQuantityRemaining())
        .setQuantityFilled(0)
        .setMatchNumber(0);

    return builder;
  }

  @Override
  public String toString() {
    return "Matcher [bidOrderList=" + bidOrderList + ", askOrderList=" + askOrderList + "]";
  }
}