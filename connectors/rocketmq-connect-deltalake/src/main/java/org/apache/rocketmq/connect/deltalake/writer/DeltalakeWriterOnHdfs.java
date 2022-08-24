package org.apache.rocketmq.connect.deltalake.writer;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;
import org.apache.rocketmq.connect.deltalake.exception.WriteParquetException;
import org.apache.rocketmq.connect.deltalake.rolling.DailyRolling;
import org.apache.rocketmq.connect.deltalake.rolling.StoreFileRolling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
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
    private ParquetWriter<GenericRecord> currentWriter;
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
            // addFile to deltalake
            if (result.isNewAdded()) {
                addOrUpdateFileToDeltaLog(result.getTableDir(), result.getFullFileName(), true, false);
            } else if (result.isNeedUpdateFile()) {
                addOrUpdateFileToDeltaLog(result.getTableDir(), result.getFullFileName(), false, true);
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
                try {
                    String fileName = storeDir + storeFile;
                    System.out.println("full path : " + fileName);
                    parquetWriter = new AvroParquetWriter(
                            new Path(fileName),
                            avroSchema,
                            CompressionCodecName.SNAPPY,
                            deltalakeConnectConfig.getBlockSize(),
                            deltalakeConnectConfig.getPageSize());
                } catch (IOException e) {
                    log.error("create parquetwriter occur exception, path : " + storeDir + storeFile + ", record : " + record, e);
                    throw new WriteParquetException("create parquetwriter occur exception, path : " + storeDir + storeFile + ", record : " + record, e);
                }
                writers.put(storeDir + storeFile, parquetWriter);
                result.setTableDir(tableDir);
                result.setFullFileName(storeDir + storeFile);
                result.setNewAdded(true);
                result.setNeedUpdateFile(false);
            }
            currentWriter =  parquetWriter;
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
        } catch (UnsupportedEncodingException e) {
            log.error("convert sinkDataEntry to GenericRecord error, record : " + record, e);
            throw new WriteParquetException("convert sinkDataEntry to GenericRecord error, record : " + record, e);
        } catch (IOException e) {
            log.error("write to parquet ioexception, record : " + record, e);
            throw new WriteParquetException("write to parquet ioexception, record : " + record, e);
        } catch (Exception e) {
            log.error("write to parquet unknown exception, record : " + record, e);
            throw new WriteParquetException("write to parquet unknown exception, record : " + record, e);
        }
        return result;
    }

    private boolean addOrUpdateFileToDeltaLog(String tablePath, String addFileName, boolean updateMeta, boolean update) {
        try {
            final String engineInfo = deltalakeConnectConfig.getEngineType();

            DeltaLog log = DeltaLog.forTable(new Configuration(), tablePath);

            // todo parse partition column
            List<String> partitionColumns = Arrays.asList("name");

            StructType schema1 = new StructType()
                    .add("name", new StringType())
                    .add("age", new IntegerType());
            System.out.println("schema : " + schema1.toJson());

            StructType schema = (StructType) StructType.fromJson(deltalakeConnectConfig.getSchema().toString(true));
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
            AddFile addFile =
                    AddFile.builder(addFileName, new HashMap<>(), status.getLen(), System.currentTimeMillis(),
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
        } catch (IOException e) {
            log.error("SinkDataEntry convert to GenericRecord occur error,", e);
        }
        return genericRecord;
    }

    public static void main(String[] args) throws Exception {

        StructType schema1 = new StructType()
                .add("name", new StringType())
                .add("age", new IntegerType());
        System.out.println("schema : " + schema1.toJson());

        File s = new File("/Users/osgoo/Downloads/user.avsc");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(s);

        System.out.println("schema : " + schema.toString());
        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "osgoo");
        user1.put("favorite_number", 256);
        user1.put("favorite_color", "white");


        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<GenericRecord>(schema);
        Encoder e = EncoderFactory.get().jsonEncoder(schema, bao);
        w.write(user1, e);
        e.flush();

        String messageContent = new String(bao.toByteArray());
        System.out.println("conent : " + new String(messageContent));
        DeltalakeConnectConfig connectConfig = new DeltalakeConnectConfig();


        GenericRecord genericRecord = new GenericData.Record(schema);
        DatumReader<GenericRecord> userDatumReader = new SpecificDatumReader<GenericRecord>(schema);
        JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema, messageContent);
        try {
            genericRecord = userDatumReader.read(genericRecord, decoder);
        } catch (IOException e2) {
            e2.printStackTrace();
            System.out.println("SinkDataEntry convert to GenericRecord occur error,");
        }

        DeltalakeWriterOnHdfs deltalakeWriterOnHdfs = new DeltalakeWriterOnHdfs(connectConfig);
        List<ConnectRecord> records = new ArrayList<>();
        RecordPartition recordPartition = convertToRecordPartition("topicName", "cluster1_broker1001", 6);
        RecordOffset recordOffset = convertToRecordOffset(1000L);
        System.out.println("recordParititon : " + recordPartition);
        System.out.println("recordOffset : " + recordOffset);
        ConnectRecord record = new ConnectRecord(recordPartition, recordOffset, System.currentTimeMillis(), null, messageContent, null, messageContent);
        records.add(record);
        try {
            deltalakeWriterOnHdfs.writeEntries(records);
        } catch (WriteParquetException e1) {
            e1.printStackTrace();
        }
    }

    public static RecordPartition convertToRecordPartition(String topic, String brokerName, int queueId) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", topic);
        map.put("brokerName", brokerName);
        map.put("queueId", queueId + "");
        RecordPartition recordPartition = new RecordPartition(map);
        return recordPartition;
    }

    public static RecordOffset convertToRecordOffset(Long offset) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(QUEUE_OFFSET, offset + "");
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";
}
