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

schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "session_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "contract_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "contract_sequence_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "bid_count",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "offer_count",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "last_trade",
    "type": "RECORD",
    "fields": [
            {
                "name": "price",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "quantity",
                "type": "INTEGER",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "mode": "REPEATED",
    "name": "bids",
    "type": "RECORD",
    "fields": [
            {
                "name": "price",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "quantity",
                "type": "INTEGER",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "mode": "REPEATED",
    "name": "offers",
    "type": "RECORD",
    "fields": [
            {
                "name": "price",
                "type": "INTEGER",
                "mode": "REQUIRED"
            },
            {
                "name": "quantity",
                "type": "INTEGER",
                "mode": "REQUIRED"
            }
    ]
  },
  {
    "mode": "REQUIRED",
    "name": "ingest_ts",
    "type": "TIMESTAMP",
    "defaultValueExpression": "CURRENT_TIMESTAMP()"
  }
]
EOF
}

resource "google_bigquery_table" "processing_status" {
deletion_protection = false
dataset_id          = google_bigquery_dataset.demo_dataset.dataset_id
table_id            = "processing_status"
  description       = "Ordered Processing Status"
  clustering        = ["session_id", "contract_id"]

schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "session_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "contract_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "status_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "received_count",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "buffered_count",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "last_processed_sequence",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "earliest_buffered_sequence",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "latest_buffered_sequence",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "duplicate_count",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "last_event_received",
    "type": "BOOLEAN"
  },
    {
    "mode": "REQUIRED",
    "name": "ingest_ts",
    "type": "TIMESTAMP",
    "defaultValueExpression": "CURRENT_TIMESTAMP()"
  }
]
EOF
}

resource "google_bigquery_table" "order_event" {
  deletion_protection = false
  dataset_id          = google_bigquery_dataset.demo_dataset.dataset_id
  table_id            = "order_event"
  description         = "Order Event"
  clustering          = ["session_id", "contract_id"]

  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "session_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "contract_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "event_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "contract_sequence_id",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "last_contract_message",
    "type": "BOOLEAN"
  },
  {
    "mode": "REQUIRED",
    "name": "order_type",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "order_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "side",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "price",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "quantity",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "quantity_remaining",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "quantity_filled",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "match_number",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "ingest_ts",
    "type": "TIMESTAMP",
    "defaultValueExpression": "CURRENT_TIMESTAMP()"
  }
]
EOF
}
