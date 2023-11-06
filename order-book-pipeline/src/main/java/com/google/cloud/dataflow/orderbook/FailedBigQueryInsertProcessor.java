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

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryStorageApiInsertError;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FailedBigQueryInsertProcessor extends
    PTransform<PCollection<BigQueryStorageApiInsertError>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(FailedBigQueryInsertProcessor.class);
  private final String table;

  FailedBigQueryInsertProcessor(String table) {
    this.table = table;
  }

  @Override
  public PDone expand(PCollection<BigQueryStorageApiInsertError> input) {
    input.apply("Report BigQuery Insert Errors", ParDo.of(
        new DoFn<BigQueryStorageApiInsertError, Boolean>() {
          @ProcessElement
          public void process(@Element BigQueryStorageApiInsertError error) {
            // In production pipelines logging errors is not recommended. A proper sink should be used instead.
            LOG.error("Failed to insert into " + table + " : " + error);
          }
        }));
    return PDone.in(input.getPipeline());
  }
}
