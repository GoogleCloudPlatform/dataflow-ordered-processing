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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.ArrayList;
import java.util.List;

public class JSONOutputConsumer implements EventConsumer {

  private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
  static {
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  @Override
  public void accept(OrderBookEvent orderBookEvent) {
    System.out.println(convert(orderBookEvent));
  }

  @Override
  public void accept(MarketDepth marketDepth) {
    System.out.println(convert(marketDepth));
  }

  @Override
  public void close() throws Exception {
    // Do nothing
  }

  private String convert(OrderBookEvent obe) {

    String timestamp = sdf.format(new Date(obe.getTimestampMS()));

    // Last message for all messages
    if (obe.getLastMessage()) {
      return String.format("{\"ts\":\"%s\",\"type\":\"last_message\",\"gseq\":%d}", obe.getSeqId());
    }

    // Last message for contract
    if (obe.getLastContractMessage()) {
      return String.format("{\"ts\":\"%s\",\"type\":\"last_contract_message\",\"gseq\":%d, \"contract\":%d, \"cseq\":%d}",
        timestamp,
        obe.getSeqId(),
        obe.getContractId(),
        obe.getContractSeqId()
      );
    }

    // Order type
    switch (obe.getType()) {
      case NEW: {
      return String.format("{\"ts\":\"%s\",\"type\":\"new_order\",\"gseq\":%d, \"contract\":%d, \"cseq\":%d, \"order_id\":%s, \"side\":\"%s\", \"remaining\":%d, \"price\":%d}",
            timestamp,
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
        return String.format("{\"ts\":\"%s\",\"type\":\"executed\",\"gseq\":%d, \"contract\":%d, \"cseq\":%d, \"order_id\":%s, \"side\":\"%s\", \"filled\":%d, \"price\":%d}",
            timestamp,
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
        return String.format("{\"ts\":\"%s\",\"type\":\"cancelled\",\"gseq\":%d, \"contract\":%d, \"cseq\":%d, \"order_id\":%s, \"side\":\"%s\", \"remaining\":%d, \"price\":%d}",
            timestamp,
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
        return String.format("BROKEN: %s", obe.toString());
      }
    }
  }

  private String convert(MarketDepth md) {

    String timestamp = sdf.format(new Date(md.getTimestampMS()));

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("{\"ts\":\"%s\",\"type\":\"marketdepth\",\"gseq\":%d, \"contract\":%d, \"cseq\":%d",
          timestamp,
          md.getSeqId(),
          md.getContractId(),
          md.getContractSeqId()));
    sb.append(",\"bids\":");
    sb.append(PriceQuantityListToString(md.getBidsList()));
    sb.append(",\"asks\":");
    sb.append(PriceQuantityListToString(md.getOffersList()));
    sb.append(",");
    if (md.getLastTrade().getQuantity() != 0) {
      sb.append(String.format("\"last_trade\":%s", PriceQuantityToString(md.getLastTrade())));
    } else {
      sb.append("\"last_trade\":{}");
    }
    sb.append("}");
    return sb.toString();
  }

  // Stringify PriceQuantity in a compact way
  private String PriceQuantityToString(MarketDepth.PriceQuantity pq) {
    return String.format("{\"quantity\":%d,\"price\":%d}", pq.getQuantity(), pq.getPrice());
  }

  // Stringify PriceQuantity List in a compact way
  private String PriceQuantityListToString(List<PriceQuantity> pqs) {
    ArrayList<String> prices = new ArrayList<String>();
    for (MarketDepth.PriceQuantity pq : pqs) {
      prices.add(PriceQuantityToString(pq));
    }
    return "[" + String.join(",", prices) + "]";
  }
}
