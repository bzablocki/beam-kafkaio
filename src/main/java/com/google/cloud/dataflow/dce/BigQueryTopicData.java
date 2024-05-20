package com.google.cloud.dataflow.dce;

import java.io.Serializable;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
public class BigQueryTopicData implements Serializable {
  private String topicName;

  public BigQueryTopicData(String topicName) {
    this.topicName = topicName;
  }

  public String getTopicName() {
    return topicName;
  }
}
