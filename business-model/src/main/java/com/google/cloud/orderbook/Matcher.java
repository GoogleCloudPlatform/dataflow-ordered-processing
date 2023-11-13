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

/*
 * Matcher for a single contract that can produce OrderBookEvents
 *
 * There must be only one Matcher class per contractId, but there can
 * be in parallel multiple matchers for different contractIds.
 */
public class Matcher {

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

  final private MatcherContext context;
  final private long contractId;
  private long matchId = 0;
  private long seqId = 1;
  private boolean isActive = true;

  // Create a matcher for a contractId
  public Matcher(MatcherContext context, long contractId) {
    this.context = context;
    this.contractId = contractId;

    this.context.addAtShutdown(() -> this.shutdown());
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

  public List<OrderBookEvent> remove(Order o) {
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

  public List<OrderBookEvent> add(Order o) {
    if (!isActive) {
      return Arrays.asList();
    }

    // Find match events
    List<OrderBookEvent> events = match(o);

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

  private List<OrderBookEvent> match(Order o) {
    if (!isActive) {
      return Arrays.asList();
    }

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

  public List<OrderBookEvent> shutdown() {
    if (!isActive) {
      return Arrays.asList();
    }

    isActive = false;

    return Arrays.asList(context.buildFinalOrderBookEvent(
      seqId++,
      contractId).build());
  }

  private Builder buildEvent(Type type, Order order) {
    return context.buildOrderBookEvent(
      type,
      seqId++,
      contractId,
      order);
  }

  @Override
  public String toString() {
    return "Matcher [bidOrderList=" + bidOrderList + ", askOrderList=" + askOrderList + "]";
  }
}