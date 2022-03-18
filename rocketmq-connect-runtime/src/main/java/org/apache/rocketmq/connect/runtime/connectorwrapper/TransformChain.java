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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.PluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransformChain<R extends ConnectRecord> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private final List<Transform> transformList;

    private final KeyValue config;

    private final Plugin plugin;

    private static final String COMMA = ",";

    private static final String PREFIX = RuntimeConfigDefine.TRANSFORMS + "-";

    public TransformChain(KeyValue config, Plugin plugin) {
        this.config = config;
        this.plugin = plugin;
        transformList = new ArrayList<>(8);
        init();
    }

    private void init() {
        String transformsStr = config.getString(RuntimeConfigDefine.TRANSFORMS);
        if (StringUtils.isBlank(transformsStr)) {
            log.warn("no transforms config, {}", JSON.toJSONString(config));
            return;
        }
        List<String> transformList = Splitter.on(COMMA).omitEmptyStrings().trimResults().splitToList(transformsStr);
        if (CollectionUtils.isEmpty(transformList)) {
            log.warn("transforms config is null, {}", JSON.toJSONString(config));
            return;
        }
        transformList.stream().forEach(transformStr -> {
            String transformClass = config.getString(PREFIX + transformStr + "-class");
            try {
                Transform transform = getTransform(transformClass);
                KeyValue transformConfig = new DefaultKeyValue();
                Set<String> configKeys = config.keySet();
                for (String key : configKeys) {
                    if (key.startsWith(PREFIX + transformStr)) {
                        key = key.replace(PREFIX + transformStr + "-", "");
                        transformConfig.put(key, config.getString(key));
                    }
                }
                transform.validate(config);
                transform.init(transformConfig);
                this.transformList.add(transform);
            } catch (Exception e) {
                log.error("transform new instance error", e);
            }
        });
    }

    public R doTransforms(R connectRecord) {
        if (transformList.size() == 0) {
            return connectRecord;
        }
        for (final Transform<R> transform : transformList) {
            final R currentRecord = connectRecord;
            connectRecord = transform.doTransform(currentRecord);
            if (connectRecord == null) {
                break;
            }
        }
        return connectRecord;
    }

    private Transform getTransform(String transformClass) throws Exception {
        ClassLoader loader = plugin.getPluginClassLoader(transformClass);
        final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
        Class transformClazz;
        boolean isolationFlag = false;
        if (loader instanceof PluginClassLoader) {
            transformClazz = ((PluginClassLoader) loader).loadClass(transformClass, false);
            isolationFlag = true;
        } else {
            transformClazz = Class.forName(transformClass);
        }
        final Transform transform = (Transform) transformClazz.getDeclaredConstructor().newInstance();
        if (isolationFlag) {
            Plugin.compareAndSwapLoaders(loader);
        }

        Plugin.compareAndSwapLoaders(currentThreadLoader);
        return transform;
    }
}
