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

TYPE=$1
SUBSCRIPTION=$2
SUFFIX="$TYPE"

if [ "$#" -ge 3 ]; then
  PIPELINE_NUMBER=$3
  SUFFIX="$TYPE-$PIPELINE_NUMBER"
fi

worker_parameters="--maxNumWorkers=30"

if [ "$#" -ge 4 ]; then
  number_of_workers=$4
  worker_parameters="--maxNumWorkers=$number_of_workers --numWorkers=$number_of_workers"
fi

JOB_NAME="data-processing-${SUFFIX}"

EXPERIMENTS=enable_recommendations,enable_lightweight_streaming_update

cd pipeline

set -x
./gradlew run --args="--jobName=${JOB_NAME} \
 --runner=Dataflow \
 --project=${PROJECT_ID} \
 --region=${GCP_REGION} \
 --enableStreamingEngine \
 --diskSizeGb=30 \
 --serviceAccount=${DATAFLOW_SA} \
 --experiments=${EXPERIMENTS} \
 --datasetName=${BQ_DATASET} \
 --tableName=${TABLE_NAME} \
 --subscription=${SUBSCRIPTION} \
 --type=${TYPE} \
 ${worker_parameters}
 "
 set +x
 cd ..

JOB_ID=$(gcloud dataflow jobs list --region "$GCP_REGION" --filter="NAME:${JOB_NAME} AND STATE:Pending" --format="get(JOB_ID)")
echo "$JOB_ID" > last_launched_pipeline.id