#
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

export PROJECT_ID=$(terraform -chdir=terraform output -raw project_id)
export REGION=$(terraform -chdir=terraform output -raw region)
export GCP_REGION=$(terraform -chdir=terraform output -raw region)
export BQ_DATASET=$(terraform -chdir=terraform output -raw bq-dataset)
export MARKET_DEPTH_TABLE_NAME=$(terraform -chdir=terraform output -raw market-depth-table-name)
export PROCESSING_STATUS_TABLE_NAME=$(terraform -chdir=terraform output -raw processing-status-table-name)
export ORDER_EVENT_TABLE_NAME=$(terraform -chdir=terraform output -raw order-event-table-name)
export ORDER_TOPIC=$(terraform -chdir=terraform output -raw order-topic)
export ORDER_SUBSCRIPTION=$(terraform -chdir=terraform output -raw order-subscription)
export MARKET_DEPTH_TOPIC=$(terraform -chdir=terraform output -raw market-depth-topic)
export MARKET_DEPTH_SUBSCRIPTION=$(terraform -chdir=terraform output -raw market-depth-subscription)
export DATAFLOW_SA=$(terraform -chdir=terraform output -raw dataflow-sa)
export DATAFLOW_TEMP_BUCKET=$(terraform -chdir=terraform output -raw dataflow-temp-bucket)