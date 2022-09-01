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

import com.google.common.collect.ImmutableMap;
import io.openmessaging.KeyValue;
import org.apache.rocketmq.connect.transforms.util.ExtendKeyValue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * pattern rename config
 */
public class PatternRenameConfig {

    public static final String FIELD_PATTERN_CONF = "field.pattern";
    static final String FIELD_PATTERN_DOC = "";

    public static final String FIELD_PATTERN_FLAGS_CONF = "field.pattern.flags";
    static final String FIELD_PATTERN_FLAGS_DOC = "";

    public static final String FIELD_REPLACEMENT_CONF = "field.replacement";
    static final String FIELD_REPLACEMENT_DOC = "";

    static final Map<String, Integer> FLAG_VALUES;

    static {
        Map<String, Integer> map = new HashMap<>();
        map.put("UNICODE_CHARACTER_CLASS", Pattern.UNICODE_CHARACTER_CLASS);
        map.put("CANON_EQ", Pattern.CANON_EQ);
        map.put("UNICODE_CASE", Pattern.UNICODE_CASE);
        map.put("DOTALL", Pattern.DOTALL);
        map.put("LITERAL", Pattern.LITERAL);
        map.put("MULTILINE", Pattern.MULTILINE);
        map.put("COMMENTS", Pattern.COMMENTS);
        map.put("CASE_INSENSITIVE", Pattern.CASE_INSENSITIVE);
        map.put("UNIX_LINES", Pattern.UNIX_LINES);
        FLAG_VALUES = ImmutableMap.copyOf(map);
    }

    public final Pattern pattern;
    public final String replacement;

    public PatternRenameConfig(KeyValue config) {
        ExtendKeyValue extendConfig = new ExtendKeyValue(config);
        final String pattern = extendConfig.getString(FIELD_PATTERN_CONF);
        final List<String> flagList = extendConfig.getList(FIELD_PATTERN_FLAGS_CONF);
        int patternFlags = 0;
        for (final String f : flagList) {
            final int flag = FLAG_VALUES.get(f);
            patternFlags = patternFlags | flag;
        }
        this.pattern = Pattern.compile(pattern, patternFlags);
        this.replacement = config.getString(FIELD_REPLACEMENT_CONF);
    }

    public Pattern pattern() {
        return pattern;
    }

    public String replacement() {
        return replacement;
    }
}
