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

import org.apache.rocketmq.connect.runtime.controller.isolation.PluginType;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginWrapper;

import java.util.Objects;

/**
 * plugin info
 */
public class PluginInfo {
    private String className;
    private PluginType type;
    private String version;

    public PluginInfo(String className, PluginType type, String version) {
        this.className = className;
        this.type = type;
        this.version = version;
    }

    public PluginInfo(PluginWrapper<?> plugin) {
        this(plugin.className(), plugin.type(), plugin.version());
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public PluginType getType() {
        return type;
    }

    public void setType(PluginType type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PluginInfo)) return false;
        PluginInfo that = (PluginInfo) o;
        return Objects.equals(className, that.className) && type == that.type && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(className, type, version);
    }

    @Override
    public String toString() {
        return "PluginInfo{" +
                "className='" + className + '\'' +
                ", type=" + type +
                ", version='" + version + '\'' +
                '}';
    }
}
