package org.apache.rocketmq.connect.deltalake.config;

import org.apache.avro.Schema;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeConnectConfig {
    private Schema schema;
    private String schemaJson;
    private int blockSize = 1024;
    private int pageSize = 65535;
    private int maxFileSize = 1024 * 1024 * 1024;
    private long parquetSegmentLength = 1024 * 1024;
    private String engineType = "hdfs";
    private String engineEndpoint = "localhost:9000";
    private String compressType = "snappy";
    // rolling file
    private String additionalPartitionColumns;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getSchemaJson() {
        return schemaJson;
    }

    public void setSchemaJson(String schemaJson) {
        this.schemaJson = schemaJson;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(int maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public long getParquetSegmentLength() {
        return parquetSegmentLength;
    }

    public void setParquetSegmentLength(long parquetSegmentLength) {
        this.parquetSegmentLength = parquetSegmentLength;
    }

    public String getEngineType() {
        return engineType;
    }

    public void setEngineType(String engineType) {
        this.engineType = engineType;
    }

    public String getEngineEndpoint() {
        return engineEndpoint;
    }

    public void setEngineEndpoint(String engineEndpoint) {
        this.engineEndpoint = engineEndpoint;
    }

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }

    public String getAdditionalPartitionColumns() {
        return additionalPartitionColumns;
    }

    public void setAdditionalPartitionColumns(String additionalPartitionColumns) {
        this.additionalPartitionColumns = additionalPartitionColumns;
    }
}
