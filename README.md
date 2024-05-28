# Reading from KafkaIO with external, dynamic topic management and automatic partition discovery.

This code sample enhances KafkaIO reading with:

- External topic management: Topics are maintained in BigQuery, providing
  flexibility.
- Dynamic partition discovery: New partitions are automatically detected and
  included.

## Key features

- BigQuery integration: Leverages BigQuery to store and manage the list of Kafka
  topics.
- Periodic topic updates: The pipeline regularly queries BigQuery for the latest
  topic list, adapting to changes.
- Dynamic partition handling: Kafka is queried for new partitions, ensuring
  comprehensive data consumption.

## How it works

1. The pipeline periodically queries a BigQuery table. In the current
   implementation the table has to have a `topic_name` column.
   for the current list of topics.
2. For the list of topics, the pipeline queries Kafka for partitions.
3. If the list of topics or partitions has changed, the pipeline dynamically
   adds them as sources.

## Important considerations

- This approach, similar to Kafka's dynamicRead, requires careful consideration
  of
  race conditions. Consult the KafkaIO documentation for details.
- Refer to the `com.google.cloud.dataflow.dce.BigQueryKafkaTopicsProvider`
  implementation for BigQuery interaction details. Any other source of truth can
  be added, as long as it implements
  the `SerializableSupplier<ImmutableList<String>>` interface.
- To change the frequency of querying the BigQuery table and kafka admin, adjust
  the parameter in the "Discover Topics and Partitions" step in the pipeline
  definition (`RunPipeline.java`). 

## How to run

Example run command:

```bash
readonly PROJECT=<your-project-id>
readonly REGION=us-central1
readonly NETWORK=regions/$REGION/subnetworks/default
readonly TEMP_LOCATION=gs://$PROJECT-beam-playground/tmp
readonly RUNNER=DataflowRunner
readonly BOOTSTRAP_SERVERS=<comma-separated list of servers, example: 10.128.0.10:9092,10.128.0.11:9092,10.128.0.12:9092>
readonly BIG_QUERY_TABLE_TOPICS_LIST=<fully qualified table name, example: my-project.my-dataset.my-table>

./gradlew run -DmainClass=com.google.cloud.dataflow.dce.RunPipeline -Pargs=" \
--streaming \
--enableStreamingEngine \
--autoscalingAlgorithm=THROUGHPUT_BASED \
--runner=$RUNNER \
--project=$PROJECT \
--tempLocation=$TEMP_LOCATION \
--region=$REGION \
--subnetwork=$NETWORK \
--experiments=use_network_tags=ssh;dataflow \
--usePublicIps=false \
--workerMachineType=n2-standard-2 \
--numWorkers=1 \
--maxNumWorkers=10 \
--kafkaBootstrapServers=$BOOTSTRAP_SERVERS \
--bigQueryTableTopicsList=$BIG_QUERY_TABLE_TOPICS_LIST"
```
