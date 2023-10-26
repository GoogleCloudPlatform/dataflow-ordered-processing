package com.google.cloud;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.MarketDepth.PriceQuantity;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.ArrayList;
import java.util.List;

public class StandardOutputConsumer implements EventConsumer {

  @Override
  public void accept(OrderBookEvent orderBookEvent) {
    System.out.println("OrderBookEvent: " + toString(orderBookEvent));
  }

  @Override
  public void accept(MarketDepth marketDepth) {
    System.out.println("MarketDepth: " + toString(marketDepth));
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  private static String toString(OrderBookEvent obe) {
    switch (obe.getType()) {
      case NEW: {
        return String.format("NEW [%d] %s %d @ %d",
            obe.getOrderId(),
            obe.getSide(),
            obe.getQuantityRemaining(),
            obe.getPrice()
        );
      }
      case EXECUTED: {
        return String.format("EXE [%d] %s %d @ %d",
            obe.getOrderId(),
            obe.getSide(),
            obe.getQuantityFilled(),
            obe.getPrice()
        );
      }
      case DELETED: {
        return String.format("CAN [%d] %s %d @ %d",
            obe.getOrderId(),
            obe.getSide(),
            obe.getQuantityRemaining(),
            obe.getPrice()
        );
      }
      default: {
        return "BROKEN";
      }
    }
  }

  private String toString(MarketDepth md) {
    StringBuilder sb = new StringBuilder();
    sb.append("Id:" + md.getContractId() + " Seq:" + md.getContractSeqId() + " ");
    sb.append("Bids: " + PriceQuantityListToString(md.getBidsList()) + " ");
    sb.append("Asks: " + PriceQuantityListToString(md.getOffersList()));
    if (md.getLastTrade().getQuantity() != 0) {
      sb.append(" LastTrade: " + PriceQuantityToString(md.getLastTrade()));
    }
    return sb.toString();
  }

  // Stringify PriceQuantity in a compact way
  private static String PriceQuantityToString(MarketDepth.PriceQuantity pq) {
    return pq.getQuantity() + "@" + pq.getPrice();
  }

  // Stringify PriceQuantity List in a compact way
  private static String PriceQuantityListToString(List<PriceQuantity> pqs) {
    ArrayList<String> prices = new ArrayList<String>();
    for (MarketDepth.PriceQuantity pq : pqs) {
      prices.add(PriceQuantityToString(pq));
    }
    return "[" + String.join(" ", prices) + "]";
  }
}
