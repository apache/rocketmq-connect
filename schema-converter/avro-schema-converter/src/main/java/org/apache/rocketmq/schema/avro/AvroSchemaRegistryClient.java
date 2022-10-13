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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.schema.common.AbstractConverterConfig;
import org.apache.rocketmq.schema.common.AbstractLocalSchemaRegistryClient;
import org.apache.rocketmq.schema.common.ParsedSchema;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;

import java.util.List;

/**
 * avro local schema registry client
 */
public class AvroSchemaRegistryClient extends AbstractLocalSchemaRegistryClient {
    public AvroSchemaRegistryClient(AbstractConverterConfig config) {
        super(config);
    }

    @Override
    protected SchemaRecordDto compareAndGet(List<SchemaRecordDto> schemaRecordAllVersion, String schemaName, ParsedSchema schema) {
        AvroSchema currentAvroSchema = (AvroSchema) schema;
        SchemaRecordDto matchSchemaRecord = null;
        for (SchemaRecordDto schemaRecord : schemaRecordAllVersion) {
            if (StringUtils.isNotEmpty(schemaRecord.getSchema()) && schemaName.equals(schemaName(schemaRecord.getSchema()))) {
                AvroSchema compareSchema = new AvroSchema(schemaRecord.getIdl());
                if (currentAvroSchema.deepEquals(compareSchema)) {
                    matchSchemaRecord = schemaRecord;
                    break;
                }
            }
        }
        return matchSchemaRecord;
    }

}
