package com.google.cloud.dataflow.orderbook;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.extensions.ordered.MutableState;

public class OrderBookMutableState implements MutableState<OrderBookEvent, MarketDepth> {


  @Override
  public void mutate(OrderBookEvent mutation) {

  }

  @Override
  public MarketDepth produceResult() {
    return null;
  }
}
