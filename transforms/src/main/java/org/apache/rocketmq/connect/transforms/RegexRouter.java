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
import io.openmessaging.connector.api.data.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * regex router
 *
 * @param <R>
 */
public abstract class RegexRouter<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger LOG = LoggerFactory.getLogger(RegexRouter.class);
    public static final String TOPIC = "topic";
    public static final String OVERVIEW_DOC = "Update the record topic using the configured regular expression and replacement string."
        + "<p/>Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. "
        + "If the pattern matches the input topic, <code>java.util.regex.Matcher#replaceFirst()</code> is used with the replacement string to obtain the new topic.";

    private interface ConfigName {
        String REGEX = "regex";
        String REPLACEMENT = "replacement";
    }

    private Pattern regex;
    private String replacement;

    @Override
    public void start(KeyValue config) {
        regex = Pattern.compile(config.getString(ConfigName.REGEX));
        replacement = config.getString(ConfigName.REPLACEMENT);
    }

    @Override
    public R doTransform(R record) {
        Map<String, Object> partitionMap = (Map<String, Object>) record.getPosition().getPartition().getPartition();
        if (null == partitionMap || !partitionMap.containsKey(TOPIC)) {
            LOG.warn("PartitionMap get topic is null , lack of topic config");
            return record;
        }
        Object o = partitionMap.get(TOPIC);
        if (null == o) {
            LOG.warn("PartitionMap get topic is null , lack of topic config");
            return record;

        }
        final Matcher matcher = regex.matcher(o.toString());
        if (matcher.matches()) {
            String topic = matcher.replaceFirst(replacement);
            partitionMap.put(TOPIC, topic);
        }
        return record;
    }
}
