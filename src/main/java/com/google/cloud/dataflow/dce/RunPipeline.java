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
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

        Class<StringDeserializer> keyDeserializer = StringDeserializer.class;
        Class<StringDeserializer> valueDeserializer = StringDeserializer.class;

        String groupIdKey = "group.id";
        String groupIdValue = "kafkaio-multiple-topics";

        // ---------------------------------
        // Protected fields from KafkaIO, when(if) this becomes part of KafkaIO, we will not need to
        // initialize it explicitly here, we will use the ones that are defined in the
        // KafkaIO.Read
        SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> kafkaConsumerFactoryFn =
                KafkaConsumer::new;

        Map<String, Object> consumerProperties =
                org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.of(
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        keyDeserializer.getName(),
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        valueDeserializer.getName(),
                        ConsumerConfig.RECEIVE_BUFFER_CONFIG,
                        512 * 1024,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                        "latest",
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                        false,
                        groupIdKey,
                        groupIdValue,
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        options.getKafkaBootstrapServers(),
                        // use when configured with 'withReadCommitted'
                        "isolation.level",
                        "read_committed");
        // ---------------------------------

        pipeline.apply(
                        "Discover Topics",
                        new SourceDescriptorDiscoverer(
                                new BigQueryKafkaTopicsProvider(
                                        options.getBigQueryTableTopicsList()),
                                new TopicToKafkaSourceDescriptorFn(
                                        kafkaConsumerFactoryFn, consumerProperties),
                                Duration.standardSeconds(5)))
                .apply(
                        "Read From Kafka",
                        KafkaIO.<String, String>readSourceDescriptors()
                                .withBootstrapServers(options.getKafkaBootstrapServers())
                                .withKeyDeserializer(keyDeserializer)
                                .withValueDeserializer(valueDeserializer)
                                .withConsumerConfigUpdates(
                                        ImmutableMap.of(groupIdKey, groupIdValue))
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
                                "Entry with key {} has {} elements.",
                                s.getKey(),
                                numberElements);

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
                        return KV.of(input.getTopic() + "_" + input.getPartition(), input);
                    }
                });
    }
}
