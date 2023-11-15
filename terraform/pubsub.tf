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


