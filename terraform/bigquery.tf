resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = "ordered_processing_demo"
  friendly_name = "Dataset to store ordered processing"
  location = var.bigquery_dataset_location
}

resource "google_bigquery_table" "order" {
  deletion_protection = false
  dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
  table_id = "order"
  description = "Synthetic orders"

  schema = <<EOF
[
  {
    "mode": "REQUIRED",
    "name": "id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "publish_ts",
    "type": "TIMESTAMP"
  },
    {
    "mode": "REQUIRED",
    "name": "ingest_ts",
    "type": "TIMESTAMP",
    "defaultValueExpression": "CURRENT_TIMESTAMP()"
  },
  {
    "mode": "REQUIRED",
    "name": "pipeline_type",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "request_ts",
    "type": "TIMESTAMP"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_sent",
    "type": "INTEGER"
  },
  {
    "mode": "REQUIRED",
    "name": "bytes_received",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "dst_hostname",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_ip",
    "type": "STRING",
    "maxLength": "15"
  },
  {
    "mode": "REQUIRED",
    "name": "dst_port",
    "type": "INTEGER"
  },
  {
    "mode": "NULLABLE",
    "name": "src_ip",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "user_id",
    "type": "STRING"
  },
  {
    "mode": "NULLABLE",
    "name": "process_name",
    "type": "STRING"
  }
]
EOF
}
