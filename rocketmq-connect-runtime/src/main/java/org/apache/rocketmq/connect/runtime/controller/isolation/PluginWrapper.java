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
package org.apache.rocketmq.connect.runtime.controller.isolation;


import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

import java.util.Objects;

/**
 * @param <T>
 */
public class PluginWrapper<T> implements Comparable<PluginWrapper<T>> {
    private final Class<? extends T> klass;
    private final String name;
    private final PluginType type;
    private final String typeName;
    private final String location;
    private final String version;
    private final DefaultArtifactVersion encodedVersion;

    private final ClassLoader classLoader;

    public PluginWrapper(Class<? extends T> klass, String version, ClassLoader loader) {
        this.klass = klass;
        this.name = klass.getName();
        this.type = PluginType.from(klass);
        this.typeName = type.toString();
        this.classLoader = loader;
        this.location = loader instanceof PluginClassLoader
                ? ((PluginClassLoader) loader).location()
                : "classpath";
        this.version = version != null ? version : "null";
        this.encodedVersion = new DefaultArtifactVersion(this.version);
    }

    public ClassLoader getClassLoader() {
        return this.classLoader;
    }

    public Class<? extends T> pluginClass() {
        return klass;
    }

    public String className() {
        return name;
    }

    public String version() {
        return version;
    }

    public PluginType type() {
        return type;
    }

    public String typeName() {
        return typeName;
    }

    public String location() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PluginWrapper)) {
            return false;
        }
        PluginWrapper<?> that = (PluginWrapper<?>) o;
        return Objects.equals(klass, that.klass) &&
                Objects.equals(version, that.version) &&
                type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(klass, version, type);
    }

    @Override
    public int compareTo(PluginWrapper other) {
        int nameComp = name.compareTo(other.name);
        return nameComp != 0 ? nameComp : encodedVersion.compareTo(other.encodedVersion);
    }

    @Override
    public String toString() {
        return "PluginWrapper{" +
                "klass=" + klass +
                ", name='" + name + '\'' +
                ", type=" + type +
                ", typeName='" + typeName + '\'' +
                ", location='" + location + '\'' +
                ", version='" + version + '\'' +
                ", encodedVersion=" + encodedVersion +
                ", classLoader=" + classLoader +
                '}';
    }
}
