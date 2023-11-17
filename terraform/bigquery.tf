# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id    = "ordered_processing_demo"
  friendly_name = "Ordered Processing Data"
  location      = var.bigquery_dataset_location
}

resource "google_bigquery_table" "market_depth" {
  deletion_protection = false
  dataset_id          = google_bigquery_dataset.demo_dataset.dataset_id
  table_id            = "market_depth"
  description         = "Market Depths"
  clustering          = ["session_id", "contract_id"]
  schema              = file("${path.module}/bigquery-schema/market-depth.json")
}

resource "google_bigquery_table" "processing_status" {
  deletion_protection = false
  dataset_id          = google_bigquery_dataset.demo_dataset.dataset_id
  table_id            = "processing_status"
  description         = "Ordered Processing Status"
  clustering          = ["session_id", "contract_id"]
  schema              = file("${path.module}/bigquery-schema/processing-status.json")
}

resource "google_bigquery_table" "order_event" {
  deletion_protection = false
  dataset_id          = google_bigquery_dataset.demo_dataset.dataset_id
  table_id            = "order_event"
  description         = "Order Event"
  clustering          = ["session_id", "contract_id"]
  schema              = file("${path.module}/bigquery-schema/order-event.json")
}
