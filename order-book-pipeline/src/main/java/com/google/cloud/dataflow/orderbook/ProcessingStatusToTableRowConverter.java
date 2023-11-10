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
import org.apache.beam.sdk.extensions.ordered.OrderedProcessingStatus;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

public class ProcessingStatusToTableRowConverter implements
    SerializableFunction<KV<Long, OrderedProcessingStatus>, TableRow> {

  @Override
  public TableRow apply(KV<Long, OrderedProcessingStatus> input) {
    TableRow result = new TableRow();
    OrderedProcessingStatus status = input.getValue();
    result.set("contract_id", input.getKey());
    result.set("status_ts", status.getStatusDate());
    result.set("received_count", status.getNumberOfReceivedEvents());
    result.set("buffered_count", status.getNumberOfBufferedEvents());
    result.set("last_processed_sequence", status.getLastProcessedSequence());
    result.set("earliest_buffered_sequence", status.getEarliestBufferedSequence());
    result.set("latest_buffered_sequence", status.getLatestBufferedSequence());
    result.set("duplicate_count", status.getDuplicateCount());
    result.set("last_event_received", status.isLastEventReceived());
    return result;
  }
}
