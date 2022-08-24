package org.apache.rocketmq.connect.deltalake.writer;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;
import io.openmessaging.connector.api.data.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.time.Duration;
import java.util.*;

import static org.apache.rocketmq.connect.deltalake.config.ConfigUtil.convertSchemaToStructType;

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
            String storeDir = storeFileRolling.generateStoreDir(record.getPosition(), record.getTimestamp());
            String storeFile = storeFileRolling.generateStoreFileName(record.getPosition(), record.getTimestamp());
            ParquetWriter<GenericRecord> parquetWriter = writers.get(storeDir + storeFile);
            if (parquetWriter == null) {
                // todo schema evolution
//            org.apache.avro.Schema avroSchema = convertSchema(record.getSchema());
                org.apache.avro.Schema avroSchema = deltalakeConnectConfig.getSchema();
                try {
                    parquetWriter = new AvroParquetWriter(
                            new Path(storeDir + storeFile),
                            avroSchema,
                            CompressionCodecName.SNAPPY,
                            deltalakeConnectConfig.getBlockSize(),
                            deltalakeConnectConfig.getPageSize());
                } catch (IOException e) {
                    log.error("create parquetwriter occur exception, path : " + storeDir + storeFile + ", record : " + record, e);
                    throw new WriteParquetException("create parquetwriter occur exception, path : " + storeDir + storeFile + ", record : " + record, e);
                }
                writers.put(storeDir + storeFile, parquetWriter);
                result.setTableDir(storeDir);
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
        String tablePathWithEngineType = deltalakeConnectConfig.getEngineType() + "://" + deltalakeConnectConfig.getEngineEndpoint() + tablePath;
        String addFileNameWithEngineType = deltalakeConnectConfig.getEngineType() + "://" + deltalakeConnectConfig.getEngineEndpoint() + addFileName;
        try {
            final String engineInfo = deltalakeConnectConfig.getEngineType();

            DeltaLog log = DeltaLog.forTable(new Configuration(), tablePathWithEngineType);

            // todo parse partition column
            List<String> partitionColumns = Arrays.asList("name");

            StructType schema = convertSchemaToStructType(deltalakeConnectConfig.getSchema());
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
            FileSystem fs = FileSystem.get(new URI(addFileNameWithEngineType), new Configuration());
            FileStatus status = fs.getFileStatus(new Path(addFileNameWithEngineType));
            AddFile addFile =
                    AddFile.builder(addFileNameWithEngineType, new HashMap<>(), status.getLen(), System.currentTimeMillis(),
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
        }
        return false;
    }

    private org.apache.avro.Schema convertSchema(Schema schema) {
        org.apache.avro.Schema avroSchema = org.apache.avro.Schema.parse(schema.toString());
        return avroSchema;
    }

    private GenericRecord sinkDataEntry2GenericRecord(ConnectRecord record) throws UnsupportedEncodingException {
        byte[] recordBytes = ((String) record.getData()).getBytes("UTF8");
        GenericRecord genericRecord = new GenericData.Record(this.deltalakeConnectConfig.getSchema());
        DatumReader<GenericRecord> userDatumReader = new SpecificDatumReader<GenericRecord>(this.deltalakeConnectConfig.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(recordBytes, null);
        try {
            if (!decoder.isEnd()) {
                genericRecord = userDatumReader.read(genericRecord, decoder);
            }
        } catch (IOException e) {
            log.error("SinkDataEntry convert to GenericRecord occur error,", e);
        }
        return genericRecord;
    }
}
