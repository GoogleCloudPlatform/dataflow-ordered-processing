package com.google.cloud.simulator;

import java.lang.System;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Order is for the simulator.
 */
public class Order {
  static private AtomicLong nextOrderId = new AtomicLong(1);

  public Order(OrderBookEvent.Side side, long price, long quantity) {
    this.side = side;
    this.price = price;
    this.quantity = quantity;
    this.quantityRemaining = quantity;
    this.orderId = nextOrderId.getAndIncrement();
  }

  final private OrderBookEvent.Side side;
  public OrderBookEvent.Side getSide() {
    return side;
  }

  final private long price;
  public long getPrice() {
    return price;
  }

  final private long quantity;
  public long getQuantity() {
    return quantity;
  }

  private long orderId = 0;
  public long getOrderId() {
    return orderId;
  }

  private long quantityRemaining;
  public long getQuantityRemaining() {
    return quantityRemaining;
  }

  public void fillOrder(long quantity) {
    this.quantityRemaining -= quantity;
  }

  @Override
  public String toString() {
    return "Order [side=" + side + ", price=" + price + ", quantity=" + quantity
        + ", orderId=" + orderId + ", quantityRemaining="
        + quantityRemaining + "]";
  }
}