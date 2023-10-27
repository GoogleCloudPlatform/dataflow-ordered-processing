package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.OrderBookEvent.Side;
import java.util.concurrent.atomic.AtomicLong;

public class OrderFactory {
  private AtomicLong currentOrderId = new AtomicLong(1);

  public Order newOrder(Side side, long price, long quantity) {
    return new Order(currentOrderId.getAndIncrement(), side, price, quantity);
  }

}
