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

package org.apache.rocketmq.connect.iotdb.replicator.source;

import java.util.List;
import java.util.Objects;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public class DeviceEntity {

    private List<String> columnNames;

    private List<String> columnTypes;

    private RowRecord rowRecord;

    private String path;

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<String> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<String> columnTypes) {
        this.columnTypes = columnTypes;
    }

    public RowRecord getRowRecord() {
        return rowRecord;
    }

    public void setRowRecord(RowRecord rowRecord) {
        this.rowRecord = rowRecord;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DeviceEntity entity = (DeviceEntity) o;

        if (!Objects.equals(columnNames, entity.columnNames))
            return false;
        if (!Objects.equals(columnTypes, entity.columnTypes))
            return false;
        if (!Objects.equals(rowRecord, entity.rowRecord))
            return false;
        return Objects.equals(path, entity.path);
    }

    @Override public int hashCode() {
        int result = columnNames != null ? columnNames.hashCode() : 0;
        result = 31 * result + (columnTypes != null ? columnTypes.hashCode() : 0);
        result = 31 * result + (rowRecord != null ? rowRecord.hashCode() : 0);
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    @Override public String toString() {
        return "DeviceEntity{" +
            "columnNames=" + columnNames +
            ", columnTypes=" + columnTypes +
            ", rowRecord=" + rowRecord +
            ", path='" + path + '\'' +
            '}';
    }
}
