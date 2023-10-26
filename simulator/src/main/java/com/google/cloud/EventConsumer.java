package com.google.cloud;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;

public interface EventConsumer extends AutoCloseable {
  void accept(OrderBookEvent orderBookEvent);
  void accept(MarketDepth marketDepth);
}
