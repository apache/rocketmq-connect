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

package org.apache.rocketmq.schema.avro;

import org.apache.avro.Schema;
import org.apache.rocketmq.schema.common.ParsedSchema;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * avro schema
 */
public class AvroSchema implements ParsedSchema<Schema> {

    private static final Logger log = LoggerFactory.getLogger(AvroSchema.class);

    private final Schema schemaObj;
    private final Integer version;
    private final boolean isNew;
    private String canonicalString;

    public AvroSchema(String schemaString) {
        this(schemaString, null);
    }

    public AvroSchema(String schemaString,
                      Integer version) {
        this(schemaString, version, false);
    }

    public AvroSchema(String schemaString,
                      Integer version,
                      boolean isNew) {
        this.isNew = isNew;
        Schema.Parser parser = getParser();
        this.schemaObj = parser.parse(schemaString);
        this.version = version;
    }

    public AvroSchema(Schema schemaObj) {
        this(schemaObj, null);
    }

    public AvroSchema(Schema schemaObj, Integer version) {
        this.isNew = false;
        this.schemaObj = schemaObj;
        this.version = version;
    }

    private AvroSchema(
            Schema schemaObj,
            String canonicalString,
            Integer version,
            boolean isNew
    ) {
        this.isNew = isNew;
        this.schemaObj = schemaObj;
        this.canonicalString = canonicalString;
        this.version = version;
    }

    public AvroSchema copy() {
        return new AvroSchema(
                this.schemaObj,
                this.canonicalString,
                this.version,
                this.isNew
        );
    }

    @Override
    public Schema rawSchema() {
        return schemaObj;
    }

    @Override
    public SchemaType schemaType() {
        return SchemaType.AVRO;
    }

    @Override
    public String name() {
        if (schemaObj != null && schemaObj.getType() == Schema.Type.RECORD) {
            return schemaObj.getFullName();
        }
        return null;
    }

    @Override
    public String idl() {
        if (schemaObj == null) {
            return null;
        }
        if (canonicalString == null) {
            Schema.Parser parser = getParser();
            canonicalString = schemaObj.toString(false);
        }
        return canonicalString;
    }

    @Override
    public Integer version() {
        return version;
    }

    private Schema.Parser getParser() {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(isNew());
        return parser;
    }

    public boolean isNew() {
        return isNew;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AvroSchema that = (AvroSchema) o;
        return Objects.equals(idl(), that.idl())
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idl(), version);
    }

    @Override
    public String toString() {
        return idl();
    }
}
