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

package org.apache.rocketmq.connect.deltalake.config;

import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeConnectConfig {
    private Schema schema;
    private String schemaPath = "/Users/osgoo/Downloads/user.avsc";
    private int blockSize = 1024;
    private int pageSize = 65535;
    private int maxFileSize = 1024 * 1024 * 1024;
    private long parquetSegmentLength = 1024 * 1024;
    private String engineType = "hdfs";
    private String engineEndpoint = "localhost:9000";
    private String compressType = "snappy";
    // rolling file
    private String additionalPartitionColumns;

    public String getFsPrefix() {
        return engineType + "://" + engineEndpoint;
    }
    public Schema getSchema() {
        File s = new File("/Users/osgoo/Downloads/user.avsc");
        try {
            Schema schema = new Schema.Parser().parse(s);
            return schema;
        } catch (IOException e) {
            e.printStackTrace();
        }
//        File schemaFile = new File(schemaPath);
//        try {
//            return new Schema.Parser().parse(schemaFile);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        return null;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getSchemaPath() {
        return schemaPath;
    }

    public void setSchemaPath(String schemaPath) {
        this.schemaPath = schemaPath;
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
