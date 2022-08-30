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
package org.apache.rocketmq.connect.transforms;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.transforms.util.ExtendKeyValue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * pattern filter config
 */
public class PatternFilterConfig {

    public static final String PATTERN_CONFIG = "pattern";
    public static final String PATTERN_DOC = "The regex to test the message with. ";

    public static final String FIELD_CONFIG = "fields";
    public static final String FIELD_DOC = "The fields to transform.";

    private final Pattern pattern;
    private final Set<String> fields;

    public PatternFilterConfig(KeyValue config) {
        ExtendKeyValue extendKeyValue = new ExtendKeyValue(config);
        String pattern = extendKeyValue.getString(PATTERN_CONFIG);
        try {
            this.pattern = Pattern.compile(pattern);
        } catch (PatternSyntaxException var4) {
            throw new ConnectException(String.format("Could not compile regex '%s'.", pattern));
        }
        List<String> fields = extendKeyValue.getList(FIELD_CONFIG);
        this.fields = new HashSet<>(fields);
    }

    public Pattern pattern() {
        return this.pattern;
    }

    public Set<String> fields() {
        return this.fields;
    }
}
