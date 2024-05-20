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

import com.google.cloud.dataflow.dce.WatchForKafkaTopicPartitions.ConvertToDescriptor;
import com.google.cloud.dataflow.dce.WatchForKafkaTopicPartitions.WatchPartitionFn;
import com.google.cloud.dataflow.dce.options.MyRunOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class RunPipeline {

    public static class EmitSourceDescriptor extends DoFn<byte[], KafkaSourceDescriptor> {

        private final PCollectionView<Iterable<KafkaSourceDescriptor>> sourceDescriptorsSideInput;

        public EmitSourceDescriptor(
                PCollectionView<Iterable<KafkaSourceDescriptor>> sourceDescriptorsSideInput) {
            this.sourceDescriptorsSideInput = sourceDescriptorsSideInput;
        }

        @ProcessElement
        public void processElement(
                ProcessContext c, OutputReceiver<KafkaSourceDescriptor> outputReceiver) {
            Iterable<KafkaSourceDescriptor> kafkaSourceDescriptors =
                    c.sideInput(sourceDescriptorsSideInput);
            for (KafkaSourceDescriptor sourceDescriptor : kafkaSourceDescriptors) {
                outputReceiver.output(sourceDescriptor);
            }
        }
    }

    public static class MyKafkaConsumer<K, V> extends KafkaConsumer<K, V> {

        public MyKafkaConsumer(Map<String, Object> configs) {
            super(configs);
            StringBuilder sb = new StringBuilder();
            sb.append("bzablocki creating MyKafkaConsumer:\n");
            for (Entry<String, Object> entry : configs.entrySet()) {
                try {
                    sb.append("    bzablocki entry ")
                            .append(entry.getKey())
                            .append(":")
                            .append(entry.getValue())
                            .append("\n");

                } catch (Exception e) {
                    sb.append("    bzablocki value for ")
                            .append(entry.getKey())
                            .append(" is not string")
                            .append("\n");
                }
            }
            System.out.println(sb);
        }

        public MyKafkaConsumer(Properties properties) {
            super(properties);
        }

        public MyKafkaConsumer(
                Properties properties,
                Deserializer<K> keyDeserializer,
                Deserializer<V> valueDeserializer) {
            super(properties, keyDeserializer, valueDeserializer);
        }

        public MyKafkaConsumer(
                Map<String, Object> configs,
                Deserializer<K> keyDeserializer,
                Deserializer<V> valueDeserializer) {
            super(configs, keyDeserializer, valueDeserializer);
        }
    }

    public static void main(String[] args) {
        // List<KafkaSourceDescriptor> sourceDescriptors = new ArrayList<>();
        // for (int i = 0; i < 10; i++) {
        //     sourceDescriptors.add(
        //             KafkaSourceDescriptor.of(
        //                     new TopicPartition("test-topic-" + i, 0),
        //                     null,
        //                     null,
        //                     null,
        //                     null,
        //                     null));
        // }

        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        MyRunOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(MyRunOptions.class);
        System.out.println(options);

        Pipeline pipeline = Pipeline.create(options);
        // Create a side input that updates every 5 seconds.
        // View as an iterable, not singleton, so that if we happen to trigger more
        // than once before Latest.globally is computed we can handle both elements.

        pipeline
                // .apply(Create.of(sourceDescriptors))
                .apply(Impulse.create())
                .apply(
                        "Match new TopicPartitions",
                        Watch.growthOf(new WatchPartitionFn())
                                .withPollInterval(Duration.standardSeconds(5)))
                .apply(ParDo.of(new ConvertToDescriptor()))
                // .apply(ParDo.of(new ConvertToDescriptor(checkStopReadingFn, startReadTime,
                // stopReadTime)))
                // .apply(ParDo.of(new EmitSourceDescriptor(kafkaSourceDescriptorsSideInput)))
                // pipeline.apply(Impulse.create())
                .apply(
                        "Read From Kafka",
                        KafkaIO.<String, String>readSourceDescriptors()
                                .withConsumerFactoryFn(MyKafkaConsumer::new)
                                .withBootstrapServers(
                                        "10.128.0.37:9092,10.128.0.34:9092,10.128.0.36:9092")
                                // .withBootstrapServers("kafka-cluster-5-m-0:9092")
                                // .withTopic("test-topic-01") // use withTopics(List<String>) to
                                // read
                                // from multiple topics.
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class)

                                // Above four are required configuration. returns
                                // PCollection<KafkaRecord<String, String>>

                                // Rest of the settings are optional :

                                // you can further customize KafkaConsumer used to read the records
                                // by adding more
                                // settings for ConsumerConfig. e.g :
                                .withConsumerConfigUpdates(
                                        ImmutableMap.of("group.id", "kafkaio-multiple-topics"))

                                // set event times and watermark based on 'LogAppendTime'. To
                                // provide a custom
                                // policy see withTimestampPolicyFactory(). withProcessingTime() is
                                // the default.
                                // Use withCreateTime() with topics that have 'CreateTime'
                                // timestamps.
                                // .withLogAppendTime()
                                .withCreateTime()
                                // restrict reader to committed messages on Kafka (see method
                                // documentation).
                                .withReadCommitted()

                        // offset consumed by the pipeline can be committed back.
                        // .commitOffsetsInFinalize() //commitOffsetsInFinalize() is
                        // enabled, but group.id in Kafka consumer config is not set. Offset
                        // management requires group.id.

                        // Specified a serializable function which can determine whether to
                        // stop reading from given
                        // TopicPartition during runtime. Note that only {@link
                        // ReadFromKafkaDoFn} respect the
                        // signal.
                        // .withCheckStopReadingFn(new SerializedFunction<TopicPartition,
                        // Boolean>() {})

                        // If you would like to send messages that fail to be parsed from
                        // Kafka to an alternate sink,
                        // use the error handler pattern as defined in {@link ErrorHandler}
                        // .withBadRecordErrorHandler(errorHandler)

                        // finally, if you don't need Kafka metadata, you can drop it.g
                        // .withoutMetadata() // PCollection<KV<String, String>>
                        )
                .apply(
                        "To KV",
                        MapElements.via(
                                new SimpleFunction<
                                        KafkaRecord<String, String>,
                                        KV<String, KafkaRecord<String, String>>>() {
                                    @Override
                                    public KV<String, KafkaRecord<String, String>> apply(
                                            KafkaRecord<String, String> input) {
                                        return KV.of(input.getTopic(), input);
                                    }
                                }))
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply(GroupByKey.create())
                .apply(
                        "PassThrough",
                        MapElements.via(
                                new SimpleFunction<
                                        KV<String, Iterable<KafkaRecord<String, String>>>,
                                        KV<String, Iterable<KafkaRecord<String, String>>>>() {
                                    @Override
                                    public KV<String, Iterable<KafkaRecord<String, String>>> apply(
                                            KV<String, Iterable<KafkaRecord<String, String>>> s) {

                                        int numElems = Iterators.size(s.getValue().iterator());
                                        System.out.println(
                                                s.getKey() + ": " + numElems + " elems.");
                                        return s;
                                    }
                                }));
        pipeline.run().waitUntilFinish();
    }
}
