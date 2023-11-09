output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "dataflow-temp-bucket" {
  value = "gs://${google_storage_bucket.dataflow-temp.name}"
}

output "bq-dataset" {
  value = google_bigquery_dataset.demo_dataset.dataset_id
}

output "market-depth-table-name" {
  value = google_bigquery_table.market_depth.table_id
}

output "processing-status-table-name" {
  value = google_bigquery_table.processing_status.table_id
}

output "order-event-table-name" {
  value = google_bigquery_table.order_event.table_id
}

output "order-topic" {
  value = google_pubsub_topic.order_topic.id
}

output "order-subscription" {
  value = google_pubsub_subscription.order_subscription.id
}

output "market-depth-topic" {
  value = google_pubsub_topic.market_depth_topic.id
}

output "market-depth-subscription" {
  value = google_pubsub_subscription.market_depth_subscription.id
}

output "dataflow-sa" {
  value = google_service_account.dataflow_sa.email
}