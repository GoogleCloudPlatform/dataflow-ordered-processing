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

import com.google.cloud.orderbook.model.OrderBookEvent;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ConvertOrderToKV extends DoFn<OrderBookEvent, KV<Long, KV<Long, OrderBookEvent>>> {

  @ProcessElement
  public void convert(@Element OrderBookEvent event,
      OutputReceiver<KV<Long, KV<Long, OrderBookEvent>>> outputReceiver) {
    outputReceiver.output(KV.of(event.getContractId(), KV.of(event.getContractSeqId(), event)));
  }
}