package com.google.cloud.dataflow.dce;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BigQueryHelper {
  public static List<String> getAllTopics() throws InterruptedException {
    System.out.println("bzablockilog query for topics!");
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(
                "SELECT * FROM `dce-bzablocki-01.kafka_topics.topics_01` LIMIT 10")
            // Use standard SQL syntax for queries.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(false)
            .build();

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    TableResult result = queryJob.getQueryResults();
    System.out.println("bzablockilog query result " + result.toString());

    List<String> topics =  new ArrayList<>();

    // Print all pages of the results.
    for (FieldValueList row : result.iterateAll()) {
      // String type
      String topicName = row.get("topic_name").getStringValue();
      System.out.println("bzablocki log found " + topicName );
      topics.add(topicName);
    }

    return topics;
  }

}
