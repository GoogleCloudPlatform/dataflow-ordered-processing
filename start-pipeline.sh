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

# This script accepts one required and two optional positional parameters:
# - "global-sequence|per-key-sequence" - determines sequence processing mode
# - initial number of workers
# - any value of the second parameter will disable pipeline autoscaling

set -e
set -u

function print_usage_and_exit() {
  echo "Usage: ./start-pipeline.sh per-key-sequence|global-sequence [initial_number_of_workers] [disable-autoscaling]"
  exit 1
}

source ./get-terraform-output.sh
source ./get-pipeline-details.sh

initial_number_of_workers=1
scaling_parameters="--maxNumWorkers=50"

if [[ "$#" -eq 0 ]]; then
  print_usage_and_exit
fi

sequencing_per_key=true
sequencing_per_key_parameter=$1
if [[ $sequencing_per_key_parameter == 'global-sequence' ]]; then
  sequencing_per_key=false
elif [[ $sequencing_per_key_parameter == 'per-key-sequence' ]]; then
  sequencing_per_key=true
else
  print_usage_and_exit
fi

if [[ "$#" -ge 2 ]]; then
    initial_number_of_workers=$2
fi

if [[ "$#" -ge 3 ]]; then
    #  disable autoscaling
    scaling_parameters="--autoscalingAlgorithm=NONE"
fi

worker_parameters="${scaling_parameters} --numWorkers=${initial_number_of_workers}"

EXPERIMENTS="enable_recommendations,enable_lightweight_streaming_update"

cd order-book-pipeline

set -x
mvn -q compile exec:java -Dexec.args="--jobName=${JOB_NAME} \
 --runner=Dataflow \
 --project=${PROJECT_ID} \
 --region=${REGION} \
 --enableStreamingEngine \
 --diskSizeGb=30 \
 --serviceAccount=${DATAFLOW_SA} \
 --experiments=${EXPERIMENTS} \
 --dataflowServiceOptions=enable_streaming_engine_resource_based_billing \
 --jdkAddOpenModules=java.base/java.lang=ALL-UNNAMED \
 --marketDepthTable=${PROJECT_ID}.${BQ_DATASET}.${MARKET_DEPTH_TABLE_NAME} \
 --processingStatusTable=${PROJECT_ID}.${BQ_DATASET}.${PROCESSING_STATUS_TABLE_NAME} \
 --orderEventTable=${PROJECT_ID}.${BQ_DATASET}.${ORDER_EVENT_TABLE_NAME} \
 --subscription=${ORDER_SUBSCRIPTION} \
 --sequencingPerKey=${sequencing_per_key} \
 --tempLocation=${DATAFLOW_TEMP_BUCKET}/temp \
 ${worker_parameters}
 "
 set +x
 cd ..
