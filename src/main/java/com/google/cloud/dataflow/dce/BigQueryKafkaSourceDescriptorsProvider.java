/*
 * Copyright 2024 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.dataflow.dce;

import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.testing.SerializableMatchers.SerializableSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.TopicPartition;

public class BigQueryKafkaSourceDescriptorsProvider
        implements SerializableSupplier<ImmutableList<KafkaSourceDescriptor>> {

    private String bqTable;

    public BigQueryKafkaSourceDescriptorsProvider(String bqTable) {
        this.bqTable = bqTable;
    }

    @Override
    public ImmutableList<KafkaSourceDescriptor> get() {
        try {
            List<String> allTopics = BigQueryHelper.getAllTopics(bqTable);
            return convertStringTopicsToSourceDescriptors(allTopics);
        } catch (InterruptedException e) {
            // todo test what happens when this is thrown
            throw new RuntimeException(e);
        }
    }

    private ImmutableList<KafkaSourceDescriptor> convertStringTopicsToSourceDescriptors(
            List<String> allTopics) {
        ImmutableList.Builder<KafkaSourceDescriptor> sourceDescriptors = ImmutableList.builder();
        for (String topic : allTopics) {
            sourceDescriptors.add(
                    KafkaSourceDescriptor.of(
                            new TopicPartition(topic, 0), null, null, null, null, null));
        }
        return sourceDescriptors.build();
    }
}
