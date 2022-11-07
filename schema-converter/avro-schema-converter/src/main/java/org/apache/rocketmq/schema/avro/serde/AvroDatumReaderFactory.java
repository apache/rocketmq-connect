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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.rocketmq.schema.avro.util.AvroSchemaUtils;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AvroDatumReaderFactory extends AvroSerdeFactory {
    private final int idSize = 8;
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private final Map<String, Schema> readerSchemaCache = new ConcurrentHashMap<>();

    /**
     * datum reader cache
     */
    private final Map<SchemaPair, DatumReader<?>> datumReaderCache = new ConcurrentHashMap<>();

    private final boolean useSchemaReflection;
    private final boolean useSpecificAvroReader;
    private final boolean avroUseLogicalTypeConverters;
    private final boolean avroReflectionAllowNull;


    private AvroDatumReaderFactory(boolean useSchemaReflection, boolean avroUseLogicalTypeConverters, boolean useSpecificAvroReader, boolean avroReflectionAllowNull) {
        this.useSchemaReflection = useSchemaReflection;
        this.avroUseLogicalTypeConverters = avroUseLogicalTypeConverters;
        this.useSpecificAvroReader = useSpecificAvroReader;
        this.avroReflectionAllowNull = avroReflectionAllowNull;
    }

    /**
     * Get avro datum factory
     *
     * @return
     */
    public static AvroDatumReaderFactory get(boolean useSchemaReflection, boolean avroUseLogicalTypeConverters, boolean useSpecificAvroReader, boolean avroReflectionAllowNull) {
        return new AvroDatumReaderFactory(useSchemaReflection, avroUseLogicalTypeConverters, useSpecificAvroReader, avroReflectionAllowNull);
    }


    public Object read(ByteBuffer buffer, Schema writerSchema, Schema readerSchema) {
        DatumReader<?> reader = getDatumReader(writerSchema, readerSchema);
        int length = buffer.limit() - idSize;
        if (writerSchema.getType().equals(Schema.Type.BYTES)) {
            byte[] bytes = new byte[length];
            buffer.get(bytes, 0, length);
            return bytes;
        } else {
            int start = buffer.position() + buffer.arrayOffset();
            try {
                Object result = reader.read(null, decoderFactory.binaryDecoder(buffer.array(),
                        start, length, null));
                if (writerSchema.getType().equals(Schema.Type.STRING)) {
                    return result.toString();
                } else {
                    return result;
                }
            } catch (IOException | RuntimeException e) {
                throw new SerializationException("Error deserializing Avro message ", e);
            }
        }
    }

    private DatumReader<?> getDatumReader(Schema writerSchema, Schema readerSchema) {
        // normalize reader schema
        final Schema finalReaderSchema = getReaderSchema(writerSchema, readerSchema);
        SchemaPair cacheKey = new SchemaPair(writerSchema, finalReaderSchema);

        return datumReaderCache.computeIfAbsent(cacheKey, schema -> {
            boolean writerSchemaIsPrimitive =
                    AvroSchemaUtils.getPrimitiveSchemas().containsValue(writerSchema);
            if (writerSchemaIsPrimitive) {
                GenericData genericData = new GenericData();
                if (avroUseLogicalTypeConverters) {
                    addLogicalTypeConversion(genericData);
                }
                return new GenericDatumReader<>(writerSchema, finalReaderSchema, genericData);
            } else if (useSchemaReflection) {
                return new ReflectDatumReader<>(writerSchema, finalReaderSchema);
            } else if (useSpecificAvroReader) {
                return new SpecificDatumReader<>(writerSchema, finalReaderSchema);
            } else {
                GenericData genericData = new GenericData();
                if (avroUseLogicalTypeConverters) {
                    addLogicalTypeConversion(genericData);
                }
                return new GenericDatumReader<>(writerSchema, finalReaderSchema, genericData);
            }
        });
    }

    private Schema getReaderSchema(Schema writerSchema, Schema readerSchema) {
        if (readerSchema != null) {
            return readerSchema;
        }
        readerSchema = readerSchemaCache.get(writerSchema.getFullName());
        if (readerSchema != null) {
            return readerSchema;
        }
        boolean writerSchemaIsPrimitive =
                AvroSchemaUtils.getPrimitiveSchemas().containsValue(writerSchema);
        if (writerSchemaIsPrimitive) {
            readerSchema = writerSchema;
        } else if (useSchemaReflection) {
            readerSchema = getReflectionReaderSchema(writerSchema);
            readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
        } else if (useSpecificAvroReader) {
            readerSchema = getSpecificReaderSchema(writerSchema);
            readerSchemaCache.put(writerSchema.getFullName(), readerSchema);
        } else {
            readerSchema = writerSchema;
        }
        return readerSchema;
    }

    private Schema getSpecificReaderSchema(Schema writerSchema) {
        if (writerSchema.getType() == Schema.Type.ARRAY
                || writerSchema.getType() == Schema.Type.MAP
                || writerSchema.getType() == Schema.Type.UNION) {
            return writerSchema;
        }
        Class<SpecificRecord> readerClass = SpecificData.get().getClass(writerSchema);
        if (readerClass == null) {
            throw new SerializationException("Could not find class "
                    + writerSchema.getFullName()
                    + " specified in writer's schema whilst finding reader's "
                    + "schema for a SpecificRecord.");
        }
        try {
            return readerClass.newInstance().getSchema();
        } catch (InstantiationException e) {
            throw new SerializationException(writerSchema.getFullName()
                    + " specified by the "
                    + "writers schema could not be instantiated to "
                    + "find the readers schema.");
        } catch (IllegalAccessException e) {
            throw new SerializationException(writerSchema.getFullName()
                    + " specified by the "
                    + "writers schema is not allowed to be instantiated "
                    + "to find the readers schema.");
        }
    }

    private Schema getReflectionReaderSchema(Schema writerSchema) {
        ReflectData reflectData = avroReflectionAllowNull ? ReflectData.AllowNull.get()
                : ReflectData.get();
        Class<?> readerClass = reflectData.getClass(writerSchema);
        if (readerClass == null) {
            throw new SerializationException("Could not find class "
                    + writerSchema.getFullName()
                    + " specified in writer's schema whilst finding reader's "
                    + "schema for a reflected class.");
        }
        return reflectData.getSchema(readerClass);
    }

}
