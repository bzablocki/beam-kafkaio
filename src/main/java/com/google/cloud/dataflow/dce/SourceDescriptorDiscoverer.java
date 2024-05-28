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

import java.util.Objects;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.testing.SerializableMatchers.SerializableSupplier;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceDescriptorDiscoverer
        extends PTransform<PBegin, PCollection<KafkaSourceDescriptor>> {

    public static final Logger LOG = LoggerFactory.getLogger(SourceDescriptorDiscoverer.class);
    private final SerializableSupplier<ImmutableList<String>> kafkaSourceDescriptorsProvider;
    private SerializableFunction<String, ImmutableList<KafkaSourceDescriptor>> partitionsForTopicFn;
    private final Duration pollInterval;

    public SourceDescriptorDiscoverer(
            SerializableSupplier<ImmutableList<String>> kafkaSourceDescriptorsProvider,
            SerializableFunction<String, ImmutableList<KafkaSourceDescriptor>> partitionsForTopicFn,
            Duration pollInterval) {
        this.kafkaSourceDescriptorsProvider = kafkaSourceDescriptorsProvider;
        this.partitionsForTopicFn = partitionsForTopicFn;
        this.pollInterval = pollInterval;
    }

    @Override
    public PCollection<KafkaSourceDescriptor> expand(PBegin input) {
        return input.apply(Impulse.create())
                .apply(
                        "Query for new Topics",
                        Watch.growthOf(
                                        new WatchTopicPartitionFn(
                                                kafkaSourceDescriptorsProvider,
                                                partitionsForTopicFn))
                                .withPollInterval(pollInterval))
                .apply("Extract KafkaSourceDescriptors", ParDo.of(new ExtractSourceDescriptors()));
    }

    static class WatchTopicPartitionFn extends PollFn<byte[], KafkaSourceDescriptor> {

        private final SerializableSupplier<ImmutableList<String>> topicsSupplier;
        private final SerializableFunction<String, ImmutableList<KafkaSourceDescriptor>>
                topicToKafkaSourceDescriptorFn;

        public WatchTopicPartitionFn(
                SerializableSupplier<ImmutableList<String>> topicsSupplier,
                SerializableFunction<String, ImmutableList<KafkaSourceDescriptor>>
                        topicToKafkaSourceDescriptorFn) {
            this.topicsSupplier = topicsSupplier;
            this.topicToKafkaSourceDescriptorFn = topicToKafkaSourceDescriptorFn;
        }

        @Override
        public Watch.Growth.PollResult<KafkaSourceDescriptor> apply(byte[] element, Context c) {
            Instant now = Instant.now();
            ImmutableList<String> topics = topicsSupplier.get();
            ImmutableList<KafkaSourceDescriptor> kafkaSourceDescriptors =
                    topics.stream()
                            .map(topicToKafkaSourceDescriptorFn::apply)
                            .flatMap(ImmutableList::stream)
                            .collect(ImmutableList.toImmutableList());
            LOG.info(
                    "Discovered kafkaSourceDescriptors {}", kafkaSourceDescriptors.toString());
            return Watch.Growth.PollResult.incomplete(now, kafkaSourceDescriptors)
                    .withWatermark(now);
        }
    }

    static class ExtractSourceDescriptors
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
