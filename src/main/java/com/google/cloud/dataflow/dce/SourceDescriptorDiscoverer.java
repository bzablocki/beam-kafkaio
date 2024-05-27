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
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.Watch.Growth.PollFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class SourceDescriptorDiscoverer
        extends PTransform<PBegin, PCollection<KafkaSourceDescriptor>> {

    private final SerializableSupplier<ImmutableList<KafkaSourceDescriptor>>
            kafkaSourceDescriptorsProvider;
    private final Duration pollInterval;

    public SourceDescriptorDiscoverer(
            SerializableSupplier<ImmutableList<KafkaSourceDescriptor>>
                    kafkaSourceDescriptorsProvider,
            Duration pollInterval) {
        this.kafkaSourceDescriptorsProvider = kafkaSourceDescriptorsProvider;
        this.pollInterval = pollInterval;
    }

    @Override
    public PCollection<KafkaSourceDescriptor> expand(PBegin input) {
        return input.apply(Impulse.create())
                .apply(
                        "Query for new Topics",
                        Watch.growthOf(new WatchPartitionFn(kafkaSourceDescriptorsProvider))
                                .withPollInterval(pollInterval))
                .apply("Extract KafkaSourceDescriptors", ParDo.of(new ExtractSourceDescriptors()));
    }

    static class WatchPartitionFn extends PollFn<byte[], KafkaSourceDescriptor> {

        private final SerializableSupplier<ImmutableList<KafkaSourceDescriptor>>
                kafkaSourceDescriptorsProvider;

        public WatchPartitionFn(
                SerializableSupplier<ImmutableList<KafkaSourceDescriptor>>
                        kafkaSourceDescriptorsProvider) {
            this.kafkaSourceDescriptorsProvider = kafkaSourceDescriptorsProvider;
        }

        @Override
        public Watch.Growth.PollResult<KafkaSourceDescriptor> apply(byte[] element, Context c) {
            Instant now = Instant.now();
            return Watch.Growth.PollResult.incomplete(now, kafkaSourceDescriptorsProvider.get())
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
