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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.deltalake.writer;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.*;
import io.openmessaging.connector.api.data.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.deltalake.exception.WriteParquetException;
import org.apache.rocketmq.connect.deltalake.rolling.DailyRolling;
import org.apache.rocketmq.connect.deltalake.rolling.StoreFileRolling;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.time.Duration;
import java.util.*;


/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeWriterOnHdfs implements DeltalakeWriter {
    private Logger log = LoggerFactory.getLogger(DeltalakeWriterOnHdfs.class);
    private DeltalakeConnectConfig deltalakeConnectConfig;
    private StoreFileRolling storeFileRolling;
    private long lastUpdateAddFileInfo = System.currentTimeMillis();
    private ParquetWriter currentWriter;
    private Map<String, ParquetWriter<GenericRecord>> writers;

    public DeltalakeWriterOnHdfs(DeltalakeConnectConfig deltalakeConnectConfig) {
        this.deltalakeConnectConfig = deltalakeConnectConfig;
        storeFileRolling = new DailyRolling(deltalakeConnectConfig);
        writers = new HashMap<>();
    }

    public void writeEntries(Collection<ConnectRecord> entries) throws WriteParquetException {
        for (ConnectRecord record : entries) {
            // write to parquet file
            WriteParquetResult result = writeParquet(record);
            System.out.println("write parquet result : " + result);
            // addFile to deltalake
            if (result.isNewAdded()) {
                addOrUpdateFileToDeltaLog(result, true, false);
            } else if (result.isNeedUpdateFile()) {
                addOrUpdateFileToDeltaLog(result, false, true);
            }
        }
    }

    private WriteParquetResult checkParquetWriter(ConnectRecord record) throws WriteParquetException {
        WriteParquetResult result = new WriteParquetResult(null, null, false, false);
        if (currentWriter == null) {
            String tableDir = deltalakeConnectConfig.getFsPrefix() + storeFileRolling.generateTableDir(record.getPosition());
            String storeDir = deltalakeConnectConfig.getFsPrefix() + storeFileRolling.generateStoreDir(record.getPosition(), record.getTimestamp());
            String storeFile = storeFileRolling.generateStoreFileName(record.getPosition(), record.getTimestamp());
            System.out.println("table : " + tableDir);
            System.out.println("store : " + storeDir);
            System.out.println("store file : " + storeFile);
            ParquetWriter<GenericRecord> parquetWriter = writers.get(storeDir + storeFile);
            if (parquetWriter == null) {
                // todo schema evolution
//            org.apache.avro.Schema avroSchema = convertSchema(record.getSchema());
                org.apache.avro.Schema avroSchema = deltalakeConnectConfig.getSchema();
                String fileName = storeDir + storeFile;
                System.out.println("full file name : " + fileName);
                Path path = new Path(fileName);
                int blockSize = deltalakeConnectConfig.getBlockSize();
                int pageSize = deltalakeConnectConfig.getPageSize();
                // todo only use this format can write success parquet
                try(
                        AvroParquetWriter parquetWriterTmp = new AvroParquetWriter(
                                path,
                                avroSchema,
                                CompressionCodecName.SNAPPY,
                                blockSize,
                                pageSize)
                ) {
                    currentWriter = parquetWriterTmp;
                } catch(java.io.IOException e){
                    System.out.println(String.format("Error writing parquet file %s", e.getMessage()));
                    e.printStackTrace();
                }
                // todo use following code will not write success, only write 4 byte in parquet file
//                try {
//                    parquetWriter = new AvroParquetWriter(
//                            path,
//                            avroSchema,
//                            CompressionCodecName.SNAPPY,
//                            deltalakeConnectConfig.getBlockSize(),
//                            deltalakeConnectConfig.getPageSize());
//                    currentWriter = parquetWriter;
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

                writers.put(storeDir + storeFile, currentWriter);
                result.setTableDir(tableDir);
                result.setFullFileName(storeDir + storeFile);
                result.setNewAdded(true);
                result.setNeedUpdateFile(false);
            }
            return result;
        }
        // check if to update file meta
        if (System.currentTimeMillis() - lastUpdateAddFileInfo > Duration.ofMinutes(1).toMillis()) {
            result.setNeedUpdateFile(true);
        }
        return result;
    }

    private WriteParquetResult writeParquet(ConnectRecord record) throws WriteParquetException {
        WriteParquetResult result;
        result = checkParquetWriter(record);
        GenericRecord genericRecord;
        try {
            genericRecord = sinkDataEntry2GenericRecord(record);
            currentWriter.write(genericRecord);
        } catch (Exception e) {
            log.error("write to parquet unknown exception, record : " + record, e);
            throw new WriteParquetException("write to parquet unknown exception, record : " + record, e);
        }
        return result;
    }

    private boolean addOrUpdateFileToDeltaLog(WriteParquetResult result, boolean updateMeta, boolean update) {
        String tablePath = result.getTableDir();
        String addFileName = result.getFullFileName();
        long fileSize = result.getFileSize();
        try {
            final String engineInfo = deltalakeConnectConfig.getEngineType();

            DeltaLog log = DeltaLog.forTable(new Configuration(), tablePath);

            // todo parse partition column
            List<String> partitionColumns = Arrays.asList("name");

//            StructType schema1 = new StructType()
//                    .add("name", new StringType())
//                    .add("age", new IntegerType());
//            System.out.println("schema : " + schema1.toJson());

            org.apache.avro.Schema schema1 = deltalakeConnectConfig.getSchema();
            StructField[] structFields = new StructField[schema1.getFields().size()];
            int i = 0;
            for (org.apache.avro.Schema.Field field : schema1.getFields()) {

                SchemaConverters.SchemaType schemaType = SchemaConverters.toSqlType(field.schema());
                System.out.println("schema type : " + schemaType);
                System.out.println("field schema : " + field.schema().getType());
                DataType sparkDataType = schemaType.dataType();
                System.out.println("sparkdatatype : " + sparkDataType.json());
                io.delta.standalone.types.DataType deltaDataType = io.delta.standalone.types.DataType.fromJson(sparkDataType.json());
                System.out.println("deltadatatpye : " + deltaDataType.toJson());
                System.out.println("field name : " + field.name());
                structFields[i++] = new StructField(field.name(), deltaDataType);
            }
            StructType structType = new StructType(structFields);
            StructType schema = structType;
            Metadata metadata = Metadata.builder()
                    .schema(schema)
                    // todo add partition column
//                    .partitionColumns(partitionColumns)
                    .build();

            // update schema
            OptimisticTransaction txn = log.startTransaction();
            if (updateMeta) {
                txn.updateMetadata(metadata);
            }

            // todo add partition column to new added file
//            Map<String, String> partitionValues = new HashMap<>();
//            partitionValues.put("name", "111");
//            partitionValues.put("name", "222");

            // exec AddFile action
            FileSystem fs = FileSystem.get(new URI(addFileName), new Configuration());
            FileStatus status = fs.getFileStatus(new Path(addFileName));
            System.out.println("filestatus : " + addFileName + ", length : " + status.getLen());
            AddFile addFile =
                    AddFile.builder(addFileName, new HashMap<>(), fileSize, System.currentTimeMillis(),
                            true)
                            .build();
            Operation op;
            if (!update) {
                op = new Operation(Operation.Name.WRITE);
            } else {
                op = new Operation(Operation.Name.UPDATE);
            }
            txn.commit(Collections.singletonList(addFile), op, engineInfo);
            lastUpdateAddFileInfo = System.currentTimeMillis();
            // check if file length over maxFileSize to rolling a new file
            if (status.getLen() > deltalakeConnectConfig.getMaxFileSize()) {
                currentWriter = null;
            }
            return true;
        } catch (Exception e) {
            log.error("exec AddFile exception,", e);
            e.printStackTrace();
        }
        return false;
    }

    private org.apache.avro.Schema convertSchema(Schema schema) {
        org.apache.avro.Schema avroSchema = org.apache.avro.Schema.parse(schema.toString());
        return avroSchema;
    }

    private GenericRecord sinkDataEntry2GenericRecord(ConnectRecord record) throws UnsupportedEncodingException {
        String content = (String) record.getData();
        GenericRecord genericRecord = new GenericData.Record(this.deltalakeConnectConfig.getSchema());
        DatumReader<GenericRecord> userDatumReader = new SpecificDatumReader<GenericRecord>(this.deltalakeConnectConfig.getSchema());
        try {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(deltalakeConnectConfig.getSchema(), content);
            genericRecord = userDatumReader.read(genericRecord, decoder);
            System.out.println("genericRecord : " + genericRecord);
        } catch (IOException e) {
            log.error("SinkDataEntry convert to GenericRecord occur error,", e);
        }
        return genericRecord;
    }

}
