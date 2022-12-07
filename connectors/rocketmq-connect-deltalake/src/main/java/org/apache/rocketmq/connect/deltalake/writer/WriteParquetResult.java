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

/**
 * @author osgoo
 * @date 2022/8/24
 */
public class WriteParquetResult {
    private String tableDir;
    private String fullFileName;
    private boolean isNewAdded;
    private boolean needUpdateFile;
    private long fileSize;
    public WriteParquetResult(String tableDir, String fullFileName, boolean isNewAdded, boolean needUpdateFile) {
        this.tableDir = tableDir;
        this.fullFileName = fullFileName;
        this.isNewAdded = isNewAdded;
        this.needUpdateFile = needUpdateFile;
    }

    public String getTableDir() {
        return tableDir;
    }

    public void setTableDir(String tableDir) {
        this.tableDir = tableDir;
    }

    public String getFullFileName() {
        return fullFileName;
    }

    public void setFullFileName(String fullFileName) {
        this.fullFileName = fullFileName;
    }

    public boolean isNewAdded() {
        return isNewAdded;
    }

    public void setNewAdded(boolean newAdded) {
        isNewAdded = newAdded;
    }

    public boolean isNeedUpdateFile() {
        return needUpdateFile;
    }

    public void setNeedUpdateFile(boolean needUpdateFile) {
        this.needUpdateFile = needUpdateFile;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    @Override
    public String toString() {
        return "WriteParquetResult{" +
                "tableDir='" + tableDir + '\'' +
                ", fullFileName='" + fullFileName + '\'' +
                ", isNewAdded=" + isNewAdded +
                ", needUpdateFile=" + needUpdateFile +
                '}';
    }
}
