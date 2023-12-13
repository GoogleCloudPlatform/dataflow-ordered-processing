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

TODO: describe

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

### Start the test harness

This will start a simulator which will be generating synthetic orders and expected order book
events:

```shell
./start-pubsub-simulator.sh
```

## Analyse the data

Once the pipeline is running, you can use BigQuery console, or `bq` utility to see how the pipeline
processes the data.

#### Processing state at a glance

To see the processing state for the latest session:

```sql
WITH latest_statuses AS (SELECT s.received_count,
                                s.buffered_count,
                                s.result_count,
                                s.duplicate_count,
                                s.last_event_received
                         FROM `ordered_processing_demo.processing_status` s
                         WHERE session_id = (SELECT DISTINCT session_id
                                             FROM `ordered_processing_demo.processing_status`
                                             ORDER BY session_id DESC
    LIMIT 1)
    QUALIFY RANK() OVER (PARTITION BY session_id
   , contract_id
ORDER BY status_ts DESC, received_count DESC) = 1 )
SELECT COUNT(*)                    total_contracts,
       COUNTIF(last_event_received
           AND buffered_count = 0) fully_processed,
       SUM(received_count)         total_orders_received,
       SUM(buffered_count)         total_orders_buffered,
       SUM(result_count)           total_results_produced,
       SUM(duplicate_count)        total_duplicates
FROM latest_statuses;
```

#### See the status of processing per each contract

This query shows processing status per contract for the latest session:

```sql
SELECT *
FROM `ordered_processing_demo.processing_status`
WHERE session_id = (SELECT DISTINCT session_id
                    FROM `ordered_processing_demo.processing_status`
                    ORDER BY session_id DESC
    LIMIT 1)
    QUALIFY RANK() OVER (PARTITION BY session_id
    , contract_id
ORDER BY status_ts DESC, received_count DESC) <= 5
ORDER BY
    session_id,
    contract_id,
    status_ts DESC,
    received_count DESC
    LIMIT 300
```

### Check out the latest market depths for each contract

```sql
SELECT *
FROM `ordered_processing_demo.market_depth` QUALIFY RANK() OVER (PARTITION BY session_id, contract_id ORDER BY session_id, contract_sequence_id DESC) <= 5
ORDER BY
    session_id,
    contract_id,
    contract_sequence_id DESC
    LIMIT
    300
```

## Cleanup

```shell
./stop-pipeline.sh
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
