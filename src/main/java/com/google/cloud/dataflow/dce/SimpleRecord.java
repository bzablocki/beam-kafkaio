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
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
public class SimpleRecord {
    public String payload;
    public Long messageId;
    public Integer field1;
    public Double field2;

    public SimpleRecord() {}

    public SimpleRecord(String payload, Long messageId, Integer field1, Double field2) {
        this.payload = payload;
        this.messageId = messageId;
        this.field1 = field1;
        this.field2 = field2;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Long getMessageId() {
        return messageId;
    }

    public void setMessageId(Long messageId) {
        this.messageId = messageId;
    }

    public Integer getField1() {
        return field1;
    }

    public void setField1(Integer field1) {
        this.field1 = field1;
    }

    public Double getField2() {
        return field2;
    }

    public void setField2(Double field2) {
        this.field2 = field2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimpleRecord)) {
            return false;
        }
        SimpleRecord that = (SimpleRecord) o;
        return Objects.equals(payload, that.payload) && Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(payload, messageId);
    }

    @Override
    public String toString() {
        return "SimpleRecord{"
                + "payload='"
                + payload
                + '\''
                + ", messageId='"
                + messageId
                + '\''
                + '}';
    }
}
