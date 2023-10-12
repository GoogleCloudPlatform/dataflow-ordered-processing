package com.google.cloud;

import java.util.Map;
import java.util.TreeMap;
import com.google.cloud.simulator.MarketDepth;
import com.google.cloud.simulator.OrderBookEvent;

public class OrderBookBuilder {
  final private TreeMap<Long, Long> prices = new TreeMap<Long,Long>();

  public MarketDepth updateDepth(OrderBookEvent obe, int depth) {

    long qty = 0;
    long lastTradeQty = 0;
    long lastTradePrice = 0;
    switch (obe.getType()) {
      case NEW: {
        qty = obe.getQuantityRemaining();
        break;
      }
      case EXECUTED: {
        qty = -1 * obe.getQuantityFilled();
        lastTradeQty = obe.getQuantityFilled();
        lastTradePrice = obe.getPrice();
        break;
      }
      case DELETED: {
        qty = -1 * obe.getQuantityRemaining();
        break;
      }
      default: {
        // Nothing
      }
    }

    if (qty == 0) {
      System.out.println("Qty is 0.");
      return null;
    }

    // Adjust price for Bid side
    long price = obe.getPrice();
    if (obe.getSide() == OrderBookEvent.Side.BUY) {
      price *= -1;
    }

    // Adjust the prices at the level
    final long qtyChange = qty;
    prices.compute(price, (k, v) -> {

      // Calculate new quantity
      long newQty = ((v == null) ? 0 : (long)v) + qtyChange;

      // Return the new quantity -- null (remove) if zero
      return (newQty != 0) ? newQty : null;
    });

    MarketDepth.Builder b = MarketDepth.newBuilder()
      .setTimestampMS(obe.getTimestampMS())
      .setContractId(obe.getContractId())
      .setSeqId(obe.getSeqId())
      .setContractSeqId(obe.getContractSeqId());

    if (lastTradeQty > 0) {
      b.setLastTrade(MarketDepth.PriceQuantity.newBuilder()
        .setQuantity(lastTradeQty)
        .setPrice(lastTradePrice));
    }

    if (depth > 0) {
      // Calculate the bids
      int bids = 0;
      for (Map.Entry<Long, Long> entry : prices.headMap(0L).entrySet()) {
        b.addBids(MarketDepth.PriceQuantity.newBuilder()
          .setPrice(-1 * entry.getKey())
          .setQuantity(entry.getValue())
          .build());
        bids++;
        if (bids == depth) {
          break;
        }
      }

      // Calculate the offers
      int asks = 0;
      for (Map.Entry<Long, Long> entry : prices.tailMap(0L).entrySet()) {
        b.addOffers(MarketDepth.PriceQuantity.newBuilder()
          .setPrice(entry.getKey())
          .setQuantity(entry.getValue())
          .build());
        asks ++;
        if (asks == depth) {
          break;
        }
      }
    }

    return b.build();
  }
}
