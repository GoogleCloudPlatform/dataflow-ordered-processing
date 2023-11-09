#!/usr/bin/env bash

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

set -e
set -u

source ./get-terraform-output.sh

worker_parameters="--maxNumWorkers=3"

JOB_NAME="order-book-builder"

EXPERIMENTS=enable_recommendations,enable_lightweight_streaming_update

cd order-book-pipeline

set -x
mvn -q compile exec:java -Dexec.args="--jobName=${JOB_NAME} \
 --runner=Dataflow \
 --project=${PROJECT_ID} \
 --region=${GCP_REGION} \
 --enableStreamingEngine \
 --diskSizeGb=30 \
 --serviceAccount=${DATAFLOW_SA} \
 --experiments=${EXPERIMENTS} \
 --marketDepthTable=${PROJECT_ID}.${BQ_DATASET}.${MARKET_DEPTH_TABLE_NAME} \
 --processingStatusTable=${PROJECT_ID}.${BQ_DATASET}.${PROCESSING_STATUS_TABLE_NAME} \
 --orderEventTable=${PROJECT_ID}.${BQ_DATASET}.${ORDER_EVENT_TABLE_NAME} \
 --subscription=${ORDER_SUBSCRIPTION} \
 --tempLocation=${DATAFLOW_TEMP_BUCKET}/temp \
 ${worker_parameters}
 "
 set +x
 cd ..
