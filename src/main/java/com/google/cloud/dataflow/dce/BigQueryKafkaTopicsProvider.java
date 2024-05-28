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

import org.apache.beam.sdk.testing.SerializableMatchers.SerializableSupplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class BigQueryKafkaTopicsProvider implements SerializableSupplier<ImmutableList<String>> {

    private final String bqTable;

    /**
     * Constructor for the provider.
     * @param bqTable in a format of "<project_id>.<database>.<table>". The table has to have a 'topic_name' column.
     */
    public BigQueryKafkaTopicsProvider(String bqTable) {
        this.bqTable = bqTable;
    }

    @Override
    public ImmutableList<String> get() {
        try {
            return BigQueryHelper.getAllTopics(bqTable);
        } catch (InterruptedException e) {
            // todo test what happens when this is thrown
            throw new RuntimeException(e);
        }
    }
}
