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

package com.google.cloud.dataflow.orderbook;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.orderbook.model.MarketDepth;
import com.google.cloud.orderbook.model.MarketDepth.PriceQuantity;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

public class MarketDepthToTableRowConverter implements
    SerializableFunction<KV<SessionContractKey, MarketDepth>, TableRow> {

  @Override
  public TableRow apply(KV<SessionContractKey, MarketDepth> input) {
    MarketDepth marketDepth = input.getValue();
    TableRow result = new TableRow();
    result.set("session_id", input.getKey().getSessionId());
    result.set("contract_id", input.getKey().getContractId());
    result.set("message_id", marketDepth.getMessageId());
    result.set("contract_sequence_id", marketDepth.getContractSeqId());
    result.set("bid_count", marketDepth.getBidsCount());
    result.set("offer_count", marketDepth.getOffersCount());

    if (marketDepth.getBidsCount() > 0) {
      result.set("bids", getPriceQuantityRepeatedRows(marketDepth.getBidsList()));
    }
    if (marketDepth.getOffersCount() > 0) {
      result.set("offers", getPriceQuantityRepeatedRows(marketDepth.getOffersList()));
    }

//    TODO - check if getLastTrade can be null.
    if(marketDepth.getLastTrade() != null) {
      result.set("last_trade", priceQuantityAsTableRow(marketDepth.getLastTrade()));
    }

    return result;
  }

  private static List<TableRow> getPriceQuantityRepeatedRows(List<PriceQuantity> priceQuantities) {
    List<TableRow> result = new ArrayList<>();
    for(PriceQuantity priceQuantity : priceQuantities) {
      result.add(priceQuantityAsTableRow(priceQuantity));
    }
    return result;
  }

  private static TableRow priceQuantityAsTableRow(PriceQuantity priceQuantity) {
    return new TableRow()
        .set("price", priceQuantity.getPrice())
        .set("quantity", priceQuantity.getQuantity());
  }
}
