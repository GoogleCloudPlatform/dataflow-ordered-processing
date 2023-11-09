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
import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class OrderBookEventToTableRowConverter implements
    SerializableFunction<OrderBookEvent, TableRow> {

  @Override
  public TableRow apply(OrderBookEvent input) {
    TableRow result = new TableRow();
    result.set("contract_id", input.getContractId());
    result.set("event_ts", input.getTimestampMS());
    result.set("message_id", input.getMessageId());
    result.set("contract_sequence_id", input.getContractSeqId());
    result.set("last_contract_message", input.getLastContractMessage());
    result.set("order_type", input.getType().toString());
    result.set("order_id", input.getOrderId());
    result.set("side", input.getSide().toString());
    result.set("price", input.getPrice());
    result.set("quantity", input.getQuantity());
    result.set("quantity_remaining", input.getQuantityRemaining());
    result.set("quantity_filled", input.getQuantityFilled());
    result.set("match_number", input.getMatchNumber());
    return result;
  }
}
