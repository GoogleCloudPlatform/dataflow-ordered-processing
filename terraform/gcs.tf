resource "google_storage_bucket" "dataflow-temp" {
  name = "${var.project_id}-dataflow-ordered-processing-temp"
  uniform_bucket_level_access = true
  location = var.region
  force_destroy = true
}