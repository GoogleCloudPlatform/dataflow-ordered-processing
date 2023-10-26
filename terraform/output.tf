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

output "order-table-name" {
  value = google_bigquery_table.order.table_id
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