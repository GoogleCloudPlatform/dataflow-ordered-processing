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

    // Last message for all messages
    if (obe.getLastMessage()) {
      return String.format("GSEQ:%d LAST GLOBAL MESSAGE",
        obe.getSeqId()
      );
    }

    // Last message for contract
    if (obe.getLastContractMessage()) {
      return String.format("GSEQ:%d CONTRACT:%d CSEQ:%d LAST CONTRACT MESSAGE",
        obe.getSeqId(),
        obe.getContractId(),
        obe.getContractSeqId()
      );
    }

    switch (obe.getType()) {
      case NEW: {
        return String.format("GSEQ:%d CONTRACT:%d CSEQ:%d NEW [%d] %s %d @ %d",
            obe.getSeqId(),
            obe.getContractId(),
            obe.getContractSeqId(),
            obe.getOrderId(),
            obe.getSide(),
            obe.getQuantityRemaining(),
            obe.getPrice()
        );
      }
      case EXECUTED: {
        return String.format("GSEQ:%d CONTRACT:%d CSEQ:%d EXE [%d] %s %d @ %d",
            obe.getSeqId(),
            obe.getContractId(),
            obe.getContractSeqId(),
            obe.getOrderId(),
            obe.getSide(),
            obe.getQuantityFilled(),
            obe.getPrice()
        );
      }
      case DELETED: {
        return String.format("GSEQ:%d CONTRACT:%d CSEQ:%d CAN [%d] %s %d @ %d",
            obe.getSeqId(),
            obe.getContractId(),
            obe.getContractSeqId(),
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
