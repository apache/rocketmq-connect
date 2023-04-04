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
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Transform serial actuator, including the initialization of transform
 *
 * @param <R>
 */
public class TransformChain<R extends ConnectRecord> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final String COMMA = ",";
    private static final String PREFIX = ConnectorConfig.TRANSFORMS + ".";
    private final List<Transform> transformList;
    private final KeyValue config;
    private final Plugin plugin;
    private RetryWithToleranceOperator retryWithToleranceOperator;

    public TransformChain(KeyValue config, Plugin plugin) {
        this.config = config;
        this.plugin = plugin;
        transformList = new ArrayList<>(8);
        init();
    }

    /**
     * set retryWithToleranceOperator
     */
    public void retryWithToleranceOperator(RetryWithToleranceOperator retryWithToleranceOperator) {
        this.retryWithToleranceOperator = retryWithToleranceOperator;
    }

    private void init() {
        String transformsStr = config.getString(ConnectorConfig.TRANSFORMS);
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
            String transformClassKey = PREFIX + transformStr + ".class";
            String transformClass = config.getString(transformClassKey);
            try {
                Transform transform = newTransform(transformClass);
                KeyValue transformConfig = new DefaultKeyValue();
                Set<String> configKeys = config.keySet();
                for (String key : configKeys) {
                    if (key.startsWith(PREFIX + transformStr) && !key.equals(transformClassKey)) {
                        String originKey = key.replace(PREFIX + transformStr + ".", "");
                        transformConfig.put(originKey, config.getString(key));
                    }
                }
                transform.start(transformConfig);
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
            if (this.retryWithToleranceOperator == null) {
                connectRecord = transform.doTransform(currentRecord);
            } else {
                connectRecord = this.retryWithToleranceOperator.execute(() -> transform.doTransform(currentRecord), ErrorReporter.Stage.TRANSFORMATION, transform.getClass());
            }

            if (connectRecord == null) {
                break;
            }
        }
        return connectRecord;
    }


    private Transform newTransform(String transformClass) throws Exception {
        ClassLoader savedLoader = plugin.currentThreadLoader();
        try {
            ClassLoader loader = plugin.delegatingLoader().pluginClassLoader(transformClass);
            savedLoader = Plugin.compareAndSwapLoaders(loader);
            Class transformClazz = Utils.getContextCurrentClassLoader().loadClass(transformClass);
            final Transform transform = (Transform) transformClazz.getDeclaredConstructor().newInstance();
            return transform;
        } catch (Exception ex) {
            throw new ConnectException("Load transform failed !!", ex);
        } finally {
            Plugin.compareAndSwapLoaders(savedLoader);
        }
    }

    /**
     * close transforms
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        for (Transform transform : transformList) {
            transform.stop();
        }
    }
}
