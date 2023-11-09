resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-demo-sa"
  display_name = "Service Account to run Dataflow jobs"
}

locals {
  member_dataflow_sa = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_sa_worker" {
  member  = local.member_dataflow_sa
  project = var.project_id
  role    = "roles/dataflow.worker"
}

resource "google_bigquery_table_iam_member" "dataflow_sa_bq_editor" {
  member = local.member_dataflow_sa
  role   = "roles/bigquery.dataEditor"
  dataset_id = google_bigquery_dataset.demo_dataset.dataset_id
  for_each = tomap({
    "market_depth" = google_bigquery_table.market_depth.id,
    "processing_status" = google_bigquery_table.processing_status.id,
    "order_event" = google_bigquery_table.order_event.id
  })
  table_id = each.value
}


#resource "google_pubsub_topic_iam_member" "dataflow_sa_topic_publisher" {
#  member = local.member_dataflow_sa
#  role   = "roles/pubsub.publisher"
#  topic  = google_pubsub_topic.order_topic.name
#}

resource "google_pubsub_subscription_iam_member" "dataflow_sa_order_subscriber" {
  member       = local.member_dataflow_sa
  role         = "roles/pubsub.subscriber"
  subscription = google_pubsub_subscription.order_subscription.name
}
resource "google_pubsub_subscription_iam_member" "dataflow_sa_order_viewer" {
  member       = local.member_dataflow_sa
  role         = "roles/pubsub.viewer"
  subscription = google_pubsub_subscription.order_subscription.name
}

resource "google_storage_bucket_iam_member" "dataflow_sa_temp_bucket_admin" {
  bucket = google_storage_bucket.dataflow-temp.id
  member = local.member_dataflow_sa
  role   = "roles/storage.admin"
}