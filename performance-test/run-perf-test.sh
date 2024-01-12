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

number_of_contracts=$1
number_of_events=$2
initial_number_of_workers=$3

function pipeline_is_ready_to_process_data() {
  local project_id=$1
  local pipeline_id=$2

  local number_of_log_entries
  number_of_log_entries=$(gcloud logging read "resource.type=\"dataflow_step\"
    resource.labels.job_id=\"${pipeline_id}\"
    logName=\"projects/${project_id}/logs/dataflow.googleapis.com%2Fjob-message\"
    textPayload=\"All workers have finished the startup processes and began to receive work requests.\"" \
    --format='get(insertId)' | wc -l)

  if (( number_of_log_entries == 0 )); then
      echo "Workers for pipeline ${pipeline_id} have not yet finished the startup process."
      return 1
  fi

  if (( number_of_log_entries == 1 )); then
    echo "Workers for pipeline ${pipeline_id} have completed the startup process"
    return 0
  else
    echo "Unexpected number of log entries for pipeline ${pipeline_id}: ${number_of_log_entries}"
    exit 1;
  fi
}

function check_processing_progress() {
  local session_id=$1
  local event_count=$2

  bq query --use_legacy_sql=false --format=json  \
          "WITH
              latest_statuses AS (
              SELECT
                s.received_count,
                s.buffered_count,
                s.result_count,
                s.session_id
              FROM
                \`${PROJECT_ID}.ordered_processing_demo.processing_status\` s
              WHERE
                session_id = '${session_id}' QUALIFY RANK() OVER (PARTITION BY contract_id ORDER BY status_ts DESC, received_count DESC) = 1 )
            SELECT
              MAX(session_id) session_id,
              COUNT(*) total_contracts,
              SUM(received_count) total_orders_received,
              SUM(buffered_count) total_orders_buffered,
              SUM(result_count) total_results_produced
            FROM
              latest_statuses;" | tee last_query.json

  results_produced=$(jq -r '.[0].total_results_produced' < last_query.json)

  if [[ $results_produced = 'null' ]]; then
    return 1;
  fi

  if (( results_produced == event_count )); then
    echo "Completed processing all orders for session ${session_id}"
    return 0;
  else
    return 1;
  fi
}

function wait_for_pipeline_completion() {
  local pipeline_id=$1
  while true
  do
    sleep 15
    local current_state
    current_state=$(gcloud dataflow jobs describe "${pipeline_id}" --region "$REGION" --format='get(currentState)')
    if [[ ${current_state} = 'JOB_STATE_DRAINED' ]]; then
      echo "Pipeline ${pipeline_id} is completely drained"
      return 0;
    else
      echo "Pipeline ${pipeline_id} is in ${current_state} state"
      continue
    fi
  done
}

cd ..

source ./get-terraform-output.sh
source ./get-pipeline-details.sh

./start-pipeline.sh "${initial_number_of_workers}"

pipeline_id=$(gcloud dataflow jobs list --region "$REGION" \
--filter="NAME:${JOB_NAME} AND (STATE:Pending OR STATE:Running)" \
--format="get(JOB_ID)")

wait_start=$SECONDS
TIMEOUT_IN_SECS=600
while true
do
  sleep 15
#  echo "Checking if the new pipeline ${pipeline_id} started processing..."
  pipeline_is_ready_to_process_data "${PROJECT_ID}" "${pipeline_id}" && break
  if ((SECONDS - wait_start > TIMEOUT_IN_SECS)); then
    echo "Couldn't determine if the new pipeline is ready to process the data, but reached the timeout of $TIMEOUT_IN_SECS seconds"
    break
  fi
done

echo "Starting the simulator process - ${number_of_contracts} contracts and around ${number_of_events} events."
test_run_start=$SECONDS
./run-simulator.sh \
  --ordertopic ${ORDER_TOPIC} \
  --marketdepthtopic ${MARKET_DEPTH_TOPIC} \
  --region ${REGION} \
  --limit ${number_of_events} \
  --contracts ${number_of_contracts} | tee simulator-output.txt

session_id=$(grep 'Session id for this simulation' < simulator-output.txt | egrep -o '[[:digit:]]+.*')
number_of_simulated_events=$(grep 'Total number of orders published' < simulator-output.txt | egrep -o '[[:digit:]]+')

echo "Waiting for processing of all ${number_of_simulated_events} events for session ${session_id}"

while true
do
  sleep 15
  check_processing_progress "${session_id}" "${number_of_simulated_events}" && break;
done

test_duration_in_seconds=$(( SECONDS - test_run_start ))

./stop-pipeline.sh

cd performance-test

echo "$(date '+%Y-%m-%d %H:%M:%S') - session ${session_id} with ${number_of_simulated_events} events for ${number_of_contracts} contracts processed in ${test_duration_in_seconds} seconds by pipeline ${pipeline_id}." | tee -a test-log.txt

# Need to do it in order to string together a number of tests to be run by separate pipelines.
wait_for_pipeline_completion "${pipeline_id}"