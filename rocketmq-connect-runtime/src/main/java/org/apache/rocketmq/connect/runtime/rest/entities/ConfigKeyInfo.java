/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.rest.entities;


import java.util.List;
import java.util.Objects;

public class ConfigKeyInfo {

    private String name;
    private String type;
    private boolean required;
    private String defaultValue;
    private String importance;
    private String documentation;
    private String group;
    private int orderInGroup;
    private String width;
    private String displayName;
    private List<String> dependents;

    public ConfigKeyInfo(String name,
                         String type,
                         boolean required,
                         String defaultValue,
                         String importance,
                         String documentation,
                         String group,
                         int orderInGroup,
                         String width,
                         String displayName,
                         List<String> dependents) {
        this.name = name;
        this.type = type;
        this.required = required;
        this.defaultValue = defaultValue;
        this.importance = importance;
        this.documentation = documentation;
        this.group = group;
        this.orderInGroup = orderInGroup;
        this.width = width;
        this.displayName = displayName;
        this.dependents = dependents;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isRequired() {
        return required;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getImportance() {
        return importance;
    }

    public String getDocumentation() {
        return documentation;
    }

    public String getGroup() {
        return group;
    }

    public int getOrderInGroup() {
        return orderInGroup;
    }

    public String getWidth() {
        return width;
    }

    public String getDisplayName() {
        return displayName;
    }

    public List<String> getDependents() {
        return dependents;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConfigKeyInfo that = (ConfigKeyInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(type, that.type) &&
                Objects.equals(required, that.required) &&
                Objects.equals(defaultValue, that.defaultValue) &&
                Objects.equals(importance, that.importance) &&
                Objects.equals(documentation, that.documentation) &&
                Objects.equals(group, that.group) &&
                Objects.equals(orderInGroup, that.orderInGroup) &&
                Objects.equals(width, that.width) &&
                Objects.equals(displayName, that.displayName) &&
                Objects.equals(dependents, that.dependents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, required, defaultValue, importance, documentation, group, orderInGroup, width, displayName, dependents);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
                .append(name)
                .append(",")
                .append(type)
                .append(",")
                .append(required)
                .append(",")
                .append(defaultValue)
                .append(",")
                .append(importance)
                .append(",")
                .append(documentation)
                .append(",")
                .append(group)
                .append(",")
                .append(orderInGroup)
                .append(",")
                .append(width)
                .append(",")
                .append(displayName)
                .append(",")
                .append(dependents)
                .append("]");
        return sb.toString();
    }
}
