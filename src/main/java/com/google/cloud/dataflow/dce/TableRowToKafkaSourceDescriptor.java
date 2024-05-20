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

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.kafka.common.TopicPartition;

public class TableRowToKafkaSourceDescriptor extends DoFn<TableRow, KafkaSourceDescriptor> {
    @ProcessElement
    public void process(@Element TableRow input, OutputReceiver<KafkaSourceDescriptor> o) {
        System.out.println("bzablocki emit KafkaSourceDescriptor for " + input.get("topic_name"));
        o.output(
                KafkaSourceDescriptor.of(
                        new TopicPartition((String) input.get("topic_name"), 0),
                        null,
                        null,
                        null,
                        null,
                        null));
    }
}
