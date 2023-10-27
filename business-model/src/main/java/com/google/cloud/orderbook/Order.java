package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Order is for the simulator.
 */
public class Order {

  public Order(long orderId, OrderBookEvent.Side side, long price, long quantity) {
    this.side = side;
    this.price = price;
    this.quantity = quantity;
    this.quantityRemaining = quantity;
    this.orderId = orderId;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Order)) {
      return false;
    }
    Order order = (Order) o;
    return price == order.price && quantity == order.quantity && orderId == order.orderId
        && quantityRemaining == order.quantityRemaining && side == order.side;
  }

  @Override
  public int hashCode() {
    return Objects.hash(side, price, quantity, orderId, quantityRemaining);
  }
}