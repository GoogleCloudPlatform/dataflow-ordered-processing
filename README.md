# Building Order Books using Dataflow

[//]: # ([![Open in Cloud Shell]&#40;https://gstatic.com/cloudssh/images/open-btn.svg&#41;]&#40;https://ssh.cloud.google.com/cloudshell/editor?cloudshell_git_repo=GITHUB_URL&#41;)

Demo code for processing ordered events in Apache Beam pipelines.

## Features

This repo contain a simulation of the Order Book processing in streaming and batch Apache Beam
pipelines.
It shows how ordering processing can be done in Apache Beam at scale, provides a fully functional
pipeline, a simulator test harness and a set of scripts to visualize processing steps and the output
of the pipeline.

## Use Case

The use case is maintaining an [order book](https://en.wikipedia.org/wiki/Order_book) of security
order events (buy, sell or cancellation) and producing the security's market depth on every trade.

The market depth data can be saved to a persistent storage for and additional analysis or can be
analyzed in the same pipeline to build a streaming analytics solution.

Use case is implemented as a standalone Java module ([business model](business-model)), with the
core logic residing in
the [OrderBookBuilder](business-model/src/main/java/com/google/cloud/orderbook/OrderBookBuilder.java)
class. The [simulator](simulator) module has utilities to generate order book events simulating
financial institution trading sessions.

## Pipeline Design

The pipeline uses the Beam's state and timers to process events in order. For a detailed description
of the steps needed to implement the pipeline see [this document](docs/pipeline-design.md).

## Getting Started

1. Clone this repo and switch to the checked out directory
2. Designate or create a project to run the tests and create `terraform/terraform.tfvars` file with
   the following content:

```text
project_id = "<your project id>"
```

3. Create infrastructure to run the demo:

```shell
cd terraform
terraform init
terraform apply
cd ..
```

4. Build the project

```shell
mvn clean install
```

## Running the demo

Running the demo involves starting the simulator to generate a stream of fictitious orders, running
a Dataflow pipeline to process the orders and output results in BigQuery and running SQL queries
against the BigQuery tables to verify the results and see processing statistics.

There are two separate processing modes for the pipeline, determined by how the sequence numbers
per trades are generated. One mode is the per-key mode, where the sequence numbers increase
contiguously for each contract. The other mode is the global mode, where there the order number is a
global
contiguouly increasing sequence number. The simulator and the pipeline need to run in the same mode,
e.g., if the simulator is producing order sequence numbers in the global mode then the pipeline
needs to run in the
global sequence processing mode.

### Start the pipeline

This pipeline was tested using the JDK 11. If you have multiple JDKs installed, please set
the `JAVA_HOME` environment variable to point to the right JDK.

```shell
./start-pipeline.sh per-key-sequence|global-sequence|
```

### Run the simulator

This will start a simulator which will be generating synthetic orders and expected order book
events in the per-key mode:

```shell
./run-pubsub-simulator-per-key-sequence.sh
```

To turn the simulator in the global sequence mode:

```shell
./run-pubsub-simulator-global-sequence.sh
```

## Analyse the data

Once the pipeline is running, you can use BigQuery console, or `bq` utility to see how the pipeline
processes the data.

#### Processing state at a glance

To see the processing state for the latest session:

```sql
WITH latest_statuses AS (
   -- Stats for each contract
   SELECT s.received_count,
          s.buffered_count,
          s.result_count,
          s.duplicate_count,
          s.last_event_received
   FROM `ordered_processing_demo.processing_status` s
   WHERE
      -- Find latest session_id
      session_id = (SELECT DISTINCT session_id
                    FROM `ordered_processing_demo.processing_status`
                    ORDER BY session_id DESC
   LIMIT 1
   )
-- Most recent stats by status_id across contract_id
   QUALIFY RANK() OVER (PARTITION BY contract_id
ORDER BY status_ts DESC, received_count DESC) = 1)
SELECT COUNT(*)                   total_contracts,
       COUNTIF(last_event_received
          AND buffered_count = 0) fully_processed,
       SUM(received_count)        total_orders_received,
       SUM(buffered_count)        total_orders_buffered,
       SUM(result_count)          total_results_produced,
       SUM(duplicate_count)       total_duplicates
FROM latest_statuses;
```

#### See the status of processing per each contract

This query shows last 5 processing statuses per contract for the latest session:

```sql
SELECT *
FROM `ordered_processing_demo.processing_status`
WHERE session_id = (SELECT DISTINCT session_id
                    FROM `ordered_processing_demo.processing_status`
                    ORDER BY session_id DESC LIMIT 1)
   QUALIFY RANK() OVER (PARTITION BY contract_id
ORDER BY status_ts DESC, received_count DESC) <= 5
ORDER BY contract_id, status_ts DESC, received_count DESC LIMIT 300
```

### Check out the latest market depths for each contract

```sql
SELECT *
FROM `ordered_processing_demo.market_depth`
WHERE session_id = (SELECT DISTINCT session_id
                    FROM `ordered_processing_demo.market_depth`
                    ORDER BY session_id DESC LIMIT 1)
   QUALIFY RANK() OVER ( PARTITION BY contract_id
ORDER BY contract_sequence_id DESC) <= 5
ORDER BY contract_id, contract_sequence_id DESC LIMIT 300
```

### See the end-to-end processing performance (in seconds) for the order book

These latencies represent the differences between the time the data was generated in the simulator
to the time the data became available for querying in BigQuery. There are a number of factors
affecting these latencies - the delays in publishing the event via Pub/Sub, Dataflow autoscaling
events, percentage of the events processed out of order, and frequency of flushes in the BigQueryIO.

```sql
WITH last_session_latencies AS (SELECT APPROX_QUANTILES(TIMESTAMP_DIFF(ingest_ts, event_ts, SECOND),
                                                        100) quantiles
                                FROM `ordered_processing_demo.market_depth`
                                WHERE session_id = (SELECT DISTINCT session_id
                                                    FROM `ordered_processing_demo.processing_status`
                                                    ORDER BY session_id DESC
   LIMIT 1)
   )
SELECT quantiles[OFFSET(0)] AS min,
  quantiles[
OFFSET(20)] AS percentile20, quantiles[OFFSET(50)] AS median, quantiles[OFFSET(90)] AS percentile90, quantiles[OFFSET(100)] AS max,
FROM last_session_latencies
```

### End-to-end processing performance (in seconds) for the orders

Similar to the market depth performance, but in this case orders are saved directly into BigQuery.

```sql
WITH last_session_latencies AS (SELECT APPROX_QUANTILES(TIMESTAMP_DIFF(ingest_ts, event_ts, SECOND),
                                                        100) quantiles
                                FROM `ordered_processing_demo.order_event`
                                WHERE session_id = (SELECT DISTINCT session_id
                                                    FROM `ordered_processing_demo.processing_status`
                                                    ORDER BY session_id DESC
   LIMIT 1)
   )
SELECT quantiles[OFFSET(0)] AS min,
  quantiles[
OFFSET(20)] AS percentile20, quantiles[OFFSET(50)] AS median, quantiles[OFFSET(90)] AS percentile90, quantiles[OFFSET(100)] AS max,
FROM last_session_latencies
```

## Stop the pipeline

You can run multiple, or parallel, simulator runs to see how the pipeline works. Once you are done,

```shell
./stop-pipeline.sh
```

## Performance tests

To test pipeline performance using different number of contracts, total number of orders and
different pipeline parameters:

```shell
 ./run-perf-test.sh <number-of-contract> <total-number-of-orders> <number-of-inital-workers> <disable-horizontal-autoscaling>
```

The first three parameters are required. Any value passed as the forth parameter will disable the
autoscaling.

The script will start the pipeline, wait for the workers to be ready to process the data, run the
simulator and wait for the processing completion. It will then shut down the pipeline. The results
of the run test run are appended to the `test-log.txt` file, e.g.:

```text
2024-01-12 09:47:08 - session 2024-01-12.09:36 with 50056466 events for 3000 contracts processed in 657 seconds by pipeline 2024-01-12_09_33_55-3943530097295374913.

```

## Cleanup

```shell
terraform -chdir terraform destroy 
```

## Contributing

Contributions to this repo are always welcome and highly encouraged.

See [CONTRIBUTING](CONTRIBUTING.md) for more information how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in
this project you agree to abide by its terms. See [Code of Conduct](CODE_OF_CONDUCT.md) for more
information.

## License

Apache 2.0 - See [LICENSE](LICENSE) for more information.
