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

package org.apache.rocketmq.connect.neo4j.helper;

import java.util.HashMap;
import java.util.Map;

public class Neo4jElement {
    String primaryKey = null;
    String primaryValue = null;
    String label = null;
    String toPrimaryKey = null;
    String toPrimaryValue = null;
    String fromPrimaryKey = null;
    String fromPrimaryValue = null;
    String toLabel = null;
    String fromLabel = null;

    Map<String, Object> properties = new HashMap<>();

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public Neo4jElement() {
    }

    public Neo4jElement(String primaryKey, String primaryValue, String label) {
        this.primaryKey = primaryKey;
        this.primaryValue = primaryValue;
        this.label = label;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getToLabel() {
        return toLabel;
    }

    public void setToLabel(String toLabel) {
        this.toLabel = toLabel;
    }

    public String getFromLabel() {
        return fromLabel;
    }

    public void setFromLabel(String fromLabel) {
        this.fromLabel = fromLabel;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getPrimaryValue() {
        return primaryValue;
    }

    public void setPrimaryValue(String primaryValue) {
        this.primaryValue = primaryValue;
    }

    public String getToPrimaryKey() {
        return toPrimaryKey;
    }

    public void setToPrimaryKey(String toPrimaryKey) {
        this.toPrimaryKey = toPrimaryKey;
    }

    public String getToPrimaryValue() {
        return toPrimaryValue;
    }

    public void setToPrimaryValue(String toPrimaryValue) {
        this.toPrimaryValue = toPrimaryValue;
    }

    public String getFromPrimaryKey() {
        return fromPrimaryKey;
    }

    public void setFromPrimaryKey(String fromPrimaryKey) {
        this.fromPrimaryKey = fromPrimaryKey;
    }

    public String getFromPrimaryValue() {
        return fromPrimaryValue;
    }

    public void setFromPrimaryValue(String fromPrimaryValue) {
        this.fromPrimaryValue = fromPrimaryValue;
    }
}
