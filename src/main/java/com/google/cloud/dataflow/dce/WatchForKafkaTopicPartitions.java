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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.common.TopicPartition;
import org.joda.time.Instant;

/**
 * A {@link PTransform} for continuously querying Kafka for new partitions, and emitting those
 * topics as {@link KafkaSourceDescriptor} This transform is implemented using the {@link Watch}
 * transform, and modifications to this transform should keep that in mind.
 *
 * <p>Please see
 * https://docs.google.com/document/d/1Io49s5LBs29HJyppKG3AlR-gHz5m5PC6CqO0CCoSqLs/edit?usp=sharing
 * for design details
 */
class WatchForKafkaTopicPartitions {
    private static final String COUNTER_NAMESPACE = "watch_kafka_topics";

    static class WatchPartitionFn extends PollFn<byte[], KafkaSourceDescriptor> {

        @Override
        public Watch.Growth.PollResult<KafkaSourceDescriptor> apply(byte[] element, Context c) {
            Instant now = Instant.now();
            return Watch.Growth.PollResult.incomplete(now, getAllTopicPartitions())
                    .withWatermark(now);
        }
    }

    @VisibleForTesting
    static List<KafkaSourceDescriptor> getAllTopicPartitions() {
        List<KafkaSourceDescriptor> sourceDescriptors = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            sourceDescriptors.add(
                    KafkaSourceDescriptor.of(
                            new TopicPartition("test-topic-" + i, 0),
                            null,
                            null,
                            null,
                            null,
                            null));
        }
        return sourceDescriptors;
    }

    static class ConvertToDescriptor
            extends DoFn<KV<byte[], KafkaSourceDescriptor>, KafkaSourceDescriptor> {

        @ProcessElement
        public void processElement(
                @Element KV<byte[], KafkaSourceDescriptor> elementKV,
                OutputReceiver<KafkaSourceDescriptor> receiver) {
            KafkaSourceDescriptor kafkaSourceDescriptor =
                    Objects.requireNonNull(elementKV.getValue());
            receiver.output(kafkaSourceDescriptor);
        }
    }
}
