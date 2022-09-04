/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.doris.sink.metadata;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Schema;

import java.util.Objects;

/**
 * schema pair
 */
public class SchemaPair {
    public final Schema keySchema;
    public final Schema schema;
    public final KeyValue extensions;

    public SchemaPair(Schema keySchema, Schema schema) {
        this.keySchema = keySchema;
        this.schema = schema;
        this.extensions = null;
    }

    public SchemaPair(Schema keySchema, Schema schema, KeyValue extensions) {
        this.keySchema = keySchema;
        this.schema = schema;
        this.extensions = extensions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaPair that = (SchemaPair) o;
        return Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public String toString() {
        return String.format("<SchemaPair: %s>", schema);
    }
}
