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
package org.apache.rocketmq.connect.runtime.converter.record;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * converter type
 */
public enum ConverterType {
    KEY,
    VALUE;

    private static final Map<String, ConverterType> NAME_TO_TYPE;

    static {
        ConverterType[] types = ConverterType.values();
        Map<String, ConverterType> nameToType = new HashMap<>(types.length);
        for (ConverterType type : types) {
            nameToType.put(type.name, type);
        }
        NAME_TO_TYPE = Collections.unmodifiableMap(nameToType);
    }

    private String name;

    ConverterType() {
        this.name = this.name().toLowerCase(Locale.ROOT);
    }

    public static ConverterType withName(String name) {
        if (name == null) {
            return null;
        }
        return NAME_TO_TYPE.get(name.toLowerCase(Locale.getDefault()));
    }

    public String getName() {
        return name;
    }
}
