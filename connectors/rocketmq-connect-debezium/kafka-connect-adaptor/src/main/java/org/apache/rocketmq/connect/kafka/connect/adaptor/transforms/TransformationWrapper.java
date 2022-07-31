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

package org.apache.rocketmq.connect.kafka.connect.adaptor.transforms;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * transformation utils
 */
public class TransformationWrapper {

    private static final String DEFAULT_TRANSFORM = "kafka.transforms";
    private Map<String, String> props;

    public TransformationWrapper(Map<String, String> props) {
        this.props = props;
    }

    public List<Transformation> transformations() {
        if (!props.containsKey(DEFAULT_TRANSFORM)) {
            return Collections.emptyList();
        }
        List<String> transformAliases = Arrays.asList(props.get(DEFAULT_TRANSFORM).split(","));
        List<Transformation> transformations = new ArrayList(transformAliases.size());
        Iterator transformIterator = transformAliases.iterator();
        while (transformIterator.hasNext()) {
            String alias = (String) transformIterator.next();
            String prefix = DEFAULT_TRANSFORM + "." + alias + ".";
            try {
                Transformation<SourceRecord> transformation = (Transformation) Utils.newInstance(getClass(prefix + "type"), Transformation.class);
                Map<String, Object> configs = originalsWithPrefix(prefix);
                transformation.configure(configs);
                transformations.add(transformation);
            } catch (Exception var12) {
                throw new ConnectException(var12);
            }
        }
        return transformations;
    }

    public Map<String, Object> originalsWithPrefix(String prefix) {
        Map<String, Object> result = new ConcurrentHashMap<>();

        Iterator entryIterator = props.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<String, ?> entry = (Map.Entry) entryIterator.next();
            if ((entry.getKey()).startsWith(prefix) && (entry.getKey()).length() > prefix.length()) {
                result.put((entry.getKey()).substring(prefix.length()), entry.getValue());
            }
        }
        return result;
    }

    public Class<?> getClass(String key) {
        try {
            return (Class<?>) Class.forName(props.get(key));
        } catch (ClassNotFoundException e) {
            throw new ConnectException(e);
        }
    }

}
