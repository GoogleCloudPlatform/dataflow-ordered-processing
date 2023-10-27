package com.google.cloud.orderbook;

import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.OrderBookEvent;
import java.util.Map;
import java.util.TreeMap;

public class OrderBookBuilder {
  final private TreeMap<Long, Long> prices = new TreeMap<Long,Long>();
  private OrderBookEvent lastOrderBookEvent = null;

  public void mutate(OrderBookEvent obe) {

    // Record the last orderbook event
    lastOrderBookEvent = obe;

    // Calculate the quantity delta
    long qty = 0;
    switch (obe.getType()) {
      case NEW: {
        qty = obe.getQuantityRemaining();
        break;
      }
      case EXECUTED: {
        qty = -1 * obe.getQuantityFilled();
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

    // Skip if no delta!
    if (qty == 0) {
      return;
    }

    // Determine the price to adjust
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
  }

  // NOTE: This will produce duplicate MarketDepth events that will
  // contain the same data if the depth changes beyond the depth point,
  // or the change was simply a trade.
  public MarketDepth produceResult(int depth, boolean withTrade) {

    // Create market depth
    MarketDepth.Builder b = MarketDepth.newBuilder();

    // Fill in the depth on the bids and offers
    if (depth > 0) {
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

    // Add in the last traded price (if any)
    // NOTE -- if produceResult() isn't called on every update,
    // then you will miss trades!
    if (withTrade &&
        lastOrderBookEvent.getType().equals(OrderBookEvent.Type.EXECUTED)) {
      b.setLastTrade(MarketDepth.PriceQuantity.newBuilder()
        .setQuantity(lastOrderBookEvent.getQuantityFilled())
        .setPrice(lastOrderBookEvent.getPrice()));
    }

    // Add in the metadata
    b.setTimestampMS(lastOrderBookEvent.getTimestampMS())
     .setContractId(lastOrderBookEvent.getContractId())
     .setSeqId(lastOrderBookEvent.getSeqId())
     .setContractSeqId(lastOrderBookEvent.getContractSeqId());

    return b.build();
  }
}
