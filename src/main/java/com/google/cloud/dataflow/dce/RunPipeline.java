/*
 * Copyright 2023 Google.
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

import com.google.cloud.dataflow.dce.options.MyRunOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunPipeline {
    public static final Logger LOG = LoggerFactory.getLogger(RunPipeline.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        MyRunOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(MyRunOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(
                        "Discover Topics",
                        new SourceDescriptorDiscoverer(
                                new BigQueryKafkaSourceDescriptorsProvider(
                                        options.getBigQueryTableTopicsList()),
                                Duration.standardSeconds(5)))
                .apply(
                        "Read From Kafka",
                        KafkaIO.<String, String>readSourceDescriptors()
                                .withBootstrapServers(options.getKafkaBootstrapServers())
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)
                                .withConsumerConfigUpdates(
                                        ImmutableMap.of("group.id", "kafkaio-multiple-topics"))
                                .withCreateTime()
                                .withReadCommitted())
                .apply("To KV with topic as a key", kafkaRecordToKV())
                .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply("Group by topic", GroupByKey.create())
                .apply("Log messages", logMessages());
        pipeline.run().waitUntilFinish();
    }

    @NotNull private static MapElements<
                    KV<String, Iterable<KafkaRecord<String, String>>>,
                    KV<String, Iterable<KafkaRecord<String, String>>>>
            logMessages() {
        return MapElements.via(
                new SimpleFunction<
                        KV<String, Iterable<KafkaRecord<String, String>>>,
                        KV<String, Iterable<KafkaRecord<String, String>>>>() {
                    @Override
                    public KV<String, Iterable<KafkaRecord<String, String>>> apply(
                            KV<String, Iterable<KafkaRecord<String, String>>> s) {

                        int numberElements = Iterators.size(s.getValue().iterator());
                        LOG.info(
                                "log Entry with key {} has {} elements.",
                                s.getKey(),
                                numberElements);
                        System.out.printf(
                                "Entry with key %s has %d elements.\n", s.getKey(), numberElements);
                        return s;
                    }
                });
    }

    @NotNull private static MapElements<KafkaRecord<String, String>, KV<String, KafkaRecord<String, String>>>
            kafkaRecordToKV() {
        return MapElements.via(
                new SimpleFunction<>() {
                    @Override
                    public KV<String, KafkaRecord<String, String>> apply(
                            KafkaRecord<String, String> input) {
                        return KV.of(input.getTopic(), input);
                    }
                });
    }
}
