package org.apache.rocketmq.connect.deltalake.writer;

import io.openmessaging.connector.api.data.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.deltalake.rolling.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Map;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeWriterOnHdfs implements DeltalakeWriter {
    private Logger log = LoggerFactory.getLogger(DeltalakeWriterOnHdfs.class);
    private DeltalakeConnectConfig deltalakeConnectConfig;
    private Partition partition;
    private Map<String, ParquetWriter<GenericRecord>> writers;
    private int blockSize = 1024;
    private int pageSize = 65535;

    public DeltalakeWriterOnHdfs(DeltalakeConnectConfig deltalakeConnectConfig) {
        this.deltalakeConnectConfig = deltalakeConnectConfig;
    }

    public void writeEntries(Collection<ConnectRecord> entries) {
        for (ConnectRecord record : entries) {
            // write to parquet file
            String needAddFile = writeParquet(record);
            // addFile to deltalake
            if (needAddFile) {
                addFileToDeltaLog();
            }
        }
    }

    private String writeParquet(ConnectRecord record) {
        String storeDir = partition.storeDir(record.getPosition(), record.getTimestamp());
        String storeFile = partition.storeFileName(record.getPosition());
        ParquetWriter<GenericRecord> parquetWriter = writers.get(storeDir + storeFile);
        if (parquetWriter == null) {
            org.apache.avro.Schema avroSchema = convertSchema(record.getSchema());
            try {
                parquetWriter = new AvroParquetWriter(
                        new Path(storeDir + storeFile),
                        avroSchema,
                        CompressionCodecName.SNAPPY,
                        blockSize,
                        pageSize);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
            writers.put(storeDir + storeFile, parquetWriter);
        }
        GenericRecord genericRecord;
        try {
            genericRecord = sinkDataEntry2GenericRecord(record);
            parquetWriter.write(genericRecord);
            return true;
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean addFileToDeltaLog() {
        ;
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
