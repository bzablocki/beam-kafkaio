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

import java.util.Map;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class TopicToKafkaSourceDescriptorFn
        implements SerializableFunction<String, ImmutableList<KafkaSourceDescriptor>> {

    private final SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
            kafkaConsumerFactoryFn;
    private final Map<String, Object> consumerProperties;

    public TopicToKafkaSourceDescriptorFn(
            SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>>
                    kafkaConsumerFactoryFn,
            Map<String, Object> consumerProperties) {
        this.kafkaConsumerFactoryFn = kafkaConsumerFactoryFn;
        this.consumerProperties = consumerProperties;
    }

    @Override
    public ImmutableList<KafkaSourceDescriptor> apply(String topic) {
        return getAllTopicPartitions(topic);
    }

    private ImmutableList<KafkaSourceDescriptor> getAllTopicPartitions(String topic) {
        ImmutableList.Builder<KafkaSourceDescriptor> topicPartitions = ImmutableList.builder();
        try (Consumer<byte[], byte[]> kafkaConsumer =
                kafkaConsumerFactoryFn.apply(consumerProperties)) {
            for (PartitionInfo partition : kafkaConsumer.partitionsFor(topic)) {
                topicPartitions.add(
                        KafkaSourceDescriptor.of(
                                new TopicPartition(topic, partition.partition()),
                                null,
                                null,
                                null,
                                null,
                                null));
            }
        }
        return topicPartitions.build();
    }
}
