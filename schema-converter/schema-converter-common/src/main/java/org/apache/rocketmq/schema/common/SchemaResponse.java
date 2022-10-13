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
package org.apache.rocketmq.schema.common;


import java.util.Objects;

/**
 * auto schema generic response
 */
public class SchemaResponse {
    private String subjectName;
    private String schemaName;
    private long recordId;
    private String idl;
    public SchemaResponse(String subjectName, String schemaName, long recordId, String idl) {
        this.subjectName = subjectName;
        this.schemaName = schemaName;
        this.recordId = recordId;
        this.idl = idl;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public long getRecordId() {
        return recordId;
    }

    public void setRecordId(long recordId) {
        this.recordId = recordId;
    }

    public String getIdl() {
        return idl;
    }

    public void setIdl(String idl) {
        this.idl = idl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaResponse that = (SchemaResponse) o;
        return recordId == that.recordId && Objects.equals(subjectName, that.subjectName) && Objects.equals(schemaName, that.schemaName) && Objects.equals(idl, that.idl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subjectName, schemaName, recordId, idl);
    }

    @Override
    public String toString() {
        return "SchemaResponse{" +
                "subjectFullName='" + subjectName + '\'' +
                ", schemaFullName='" + schemaName + '\'' +
                ", recordId=" + recordId +
                ", idl='" + idl + '\'' +
                '}';
    }

    public static class Builder {
        private String subjectName;
        private String schemaName;
        private long recordId;
        private String idl;

        public Builder subjectName(String subjectName) {
            this.subjectName = subjectName;
            return this;
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder recordId(long recordId) {
            this.recordId = recordId;
            return this;
        }

        public Builder idl(String idl) {
            this.idl = idl;
            return this;
        }

        public SchemaResponse build() {
            return new SchemaResponse(subjectName, schemaName, recordId, idl);
        }
    }
}
