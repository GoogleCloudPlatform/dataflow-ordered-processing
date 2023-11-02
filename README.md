# Building Order Books using Dataflow

[//]: # ([![Open in Cloud Shell]&#40;https://gstatic.com/cloudssh/images/open-btn.svg&#41;]&#40;https://ssh.cloud.google.com/cloudshell/editor?cloudshell_git_repo=GITHUB_URL&#41;)

*Description*
Demo code for processing ordered events in Apache Beam pipelines

## Features

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
./start-simulator.sh
```

## Steps required to create ordered processing

### Transform the data into the shape needed by the OrderedProcessing transform

Data needs to be in the `KV<GrouppingKey<KV<Long,Event>>` PCollection.

### Create a function which will take the first event and create a MutableState

This function needs to implement `ProcessFunction<EventType, MutableState>` interface. This function
is also a place where you can pass the parameters needed to initialize the state.

### Create coders

Coders are needed by the OrderedProcessor transform to serialize and deserialize the data until it's
ready to be processed. There are multiple coders used by the transform:

* Mutable state coder. This coder will store the state of the order book. TODO: details
* Event coder. This coder will store the events to be buffered (out-of-sequence events)
* Key coder. In our case it's pretty simple. The key type is Long and there is an efficient coder
  for Longs - VarLongCoder.

### Create a custom transform to wrap the OrderedEventProcessing transform

This is an optional step and technically you don't need to do it. But if you do - the main pipeline
code will look more compact and the graph on the Dataflow UI will look "prettier".

### Code the pipeline

The core processing of the pipeline is very simple at this point

# Don't read below this line - needs to be updated for the current demo

[//]: # (TODO: update)

A typical rate is tens of thousands of events per second. A Dataflow pipeline
named `data-generator-<rate>`
will be started. You can simulate event load increases by starting additional pipelines. Note, that
you can't start
several pipelines with exactly the same rate because the pipeline name needs to be unique.

You can see the current publishing load by summing the rates of all active data generation
pipelines.

### Start the baseline consumption pipeline

This pipeline runs uninterrupted on a dedicated PubSub subscription. The goals are to collect the
message
processing latencies under the perfect circumstances and the unique message ids in order to later
compare them with
the pipelines being tested.

```shell
./start-baseline-pipeline.sh
```

### Start the pipeline which will be updated

```shell
./start-main-pipeline.sh
```

### Update the pipeline

We are going to use the same pipeline code to update the existing pipeline - there is no difference
in processing time

```shell
./update-pipeline.sh
```

### Analyse the data

All scripts below have time ranges defined in the beginning of the scripts, typically 20 minute
intervals.
Adjust them as needed. For example, if you would like to check for a fixed time period add these
lines after `DECLARE` statements:

```sql
SET
start_ts = TIMESTAMP '2023-06-06 08:32:00.00 America/Los_Angeles';
SET
end_ts = TIMESTAMP '2023-06-06 08:45:00.00 America/Los_Angeles';
```

#### Event latencies comparison

Use the following query to compare latency of ingestion of the baseline pipeline and the main
pipeline:

```sql
DECLARE
start_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -40 MINUTE);
DECLARE
end_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -20 MINUTE);
WITH latency AS (SELECT pipeline_type,
                        ingest_ts,
                        publish_ts,
                        TIMESTAMP_DIFF(ingest_ts, publish_ts, SECOND) latency_secs
                 FROM pipeline_update.event
                 WHERE publish_ts BETWEEN start_ts AND end_ts)
SELECT pipeline_type,
       COUNT(*)                     total_events,
       AVG(latency.latency_secs)    average_latency_secs,
       MIN(latency.latency_secs)    min_latency_secs,
       MAX(latency.latency_secs)    max_latency_secs,
       STDDEV(latency.latency_secs) std_deviation
FROM latency
GROUP BY pipeline_type
ORDER BY pipeline_type;
```

#### Missing records

To check if there were missing records:

```sql
DECLARE
start_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -40 MINUTE);
DECLARE
end_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -20 MINUTE);
SELECT COUNT(*) missed_events,
FROM pipeline_update.event base
WHERE base.publish_ts BETWEEN start_ts AND end_ts
  AND pipeline_type = 'baseline'
  AND NOT EXISTS(
        SELECT *
        FROM pipeline_update.event main
        WHERE main.publish_ts BETWEEN start_ts AND end_ts
          AND pipeline_type = 'main'
          AND base.id = main.id);
```

#### Duplicates

Duplicate events

```sql
DECLARE
start_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -40 MINUTE);
DECLARE
end_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -20 MINUTE);

SELECT id,
       pipeline_type,
       COUNT(*) event_count,
FROM pipeline_update.event base
WHERE base.publish_ts BETWEEN start_ts AND end_ts
GROUP BY id, pipeline_type
HAVING event_count > 1
```

#### Duplicate statistics

```sql
DECLARE
start_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -40 MINUTE);
DECLARE
end_ts DEFAULT TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL -20 MINUTE);
WITH counts AS (SELECT COUNT(id)          total_event_count,
                       COUNT(DISTINCT id) event_distinct_count,
                       pipeline_type,
                FROM pipeline_update.event base
                WHERE base.publish_ts BETWEEN start_ts
                          AND end_ts
                GROUP BY pipeline_type)
SELECT event_distinct_count,
       counts.total_event_count - counts.event_distinct_count AS dups_count,
       (counts.total_event_count - counts.event_distinct_count) * 100 /
       counts.event_distinct_count                               dups_percentage,
       pipeline_type
FROM counts
ORDER BY pipeline_type DESC;
```

## Cleanup

```shell
./stop-event-generation.sh
./stop-processing-pipelines.sh
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