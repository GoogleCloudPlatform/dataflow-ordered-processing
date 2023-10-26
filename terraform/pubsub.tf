resource "google_pubsub_topic" "order_topic" {
  name = "orders"
}

resource "google_pubsub_subscription" "order_subscription" {
  name = "order-sub"
  topic = google_pubsub_topic.order_topic.name
}

resource "google_pubsub_topic" "market_depth_topic" {
  name = "market-depth"
}

resource "google_pubsub_subscription" "market_depth_subscription" {
  name = "market-depth-sub"
  topic = google_pubsub_topic.market_depth_topic.name
}


