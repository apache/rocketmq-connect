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
package org.apache.rocketmq.schema.avro.serde;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroDatumWriterFactory extends AvroSerdeFactory {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    /**
     * datum writer cache
     */
    private final Map<Schema, DatumWriter<Object>> datumWriterCache = new ConcurrentHashMap<>();


    private final boolean useSchemaReflection;
    private final boolean avroUseLogicalTypeConverters;

    private AvroDatumWriterFactory(boolean useSchemaReflection, boolean avroUseLogicalTypeConverters) {
        this.useSchemaReflection = useSchemaReflection;
        this.avroUseLogicalTypeConverters = avroUseLogicalTypeConverters;
    }

    /**
     * Get avro datum factory
     *
     * @return
     */
    public static AvroDatumWriterFactory get(boolean useSchemaReflection, boolean avroUseLogicalTypeConverters) {
        return new AvroDatumWriterFactory(useSchemaReflection, avroUseLogicalTypeConverters);
    }


    public void writeDatum(ByteArrayOutputStream out, Object value, Schema rawSchema)
            throws IOException {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

        DatumWriter<Object> writer;
        writer = datumWriterCache.computeIfAbsent(rawSchema, v -> (DatumWriter<Object>) getDatumWriter(value, rawSchema));
        writer.write(value, encoder);
        encoder.flush();
    }


    /**
     * get datum writer
     *
     * @param value
     * @param schema
     * @return
     */
    private DatumWriter<?> getDatumWriter(Object value, Schema schema) {
        if (value instanceof SpecificRecord) {
            return new SpecificDatumWriter<>(schema);
        } else if (useSchemaReflection) {
            return new ReflectDatumWriter<>(schema);
        } else {
            GenericData genericData = new GenericData();
            if (avroUseLogicalTypeConverters) {
                addLogicalTypeConversion(genericData);
            }
            return new GenericDatumWriter<>(schema, genericData);
        }
    }

}
