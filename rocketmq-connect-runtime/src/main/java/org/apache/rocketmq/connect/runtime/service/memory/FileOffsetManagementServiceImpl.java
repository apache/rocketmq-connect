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

package org.apache.rocketmq.connect.runtime.service.memory;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.converter.RecordOffsetConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPartitionConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * memory offset management service impl
 */
public class FileOffsetManagementServiceImpl implements PositionManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    protected ExecutorService executor;

    /**
     * Current offset info in store.
     */
    private KeyValueStore<ExtendRecordPartition, RecordOffset> offsetStore;


    /**
     * Listeners.
     */
    private PositionUpdateListener offsetUpdateListener;

    public FileOffsetManagementServiceImpl(ConnectConfig connectConfig) {
        this.offsetStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getOffsetPath(connectConfig.getStorePathRootDir()),
                new RecordPartitionConverter(),
                new RecordOffsetConverter());
    }

    @Override
    public void start() {
        executor = Executors.newFixedThreadPool(1, ThreadUtils.newThreadFactory(
                this.getClass().getSimpleName() + "-%d", false));
        offsetStore.load();
    }

    @Override
    public void stop() {
        offsetStore.persist();
        if (executor != null) {
            executor.shutdown();
            // Best effort wait for any get() and set() tasks (and caller's callbacks) to complete.
            try {
                executor.awaitTermination(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (!executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop MemoryOffsetManagementServiceImpl. Exiting without cleanly " +
                        "shutting down pending tasks and/or callbacks.");
            }
            executor = null;
        }
    }

    @Override
    public void persist() {
        offsetStore.persist();
    }

    @Override
    public void load() {
        offsetStore.load();
    }

    @Override
    public void synchronize() {
    }

    @Override
    public Map<ExtendRecordPartition, RecordOffset> getPositionTable() {
        return offsetStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(ExtendRecordPartition partition) {
        return offsetStore.get(partition);
    }

    @Override
    public void putPosition(Map<ExtendRecordPartition, RecordOffset> offsets) {
        offsetStore.putAll(offsets);
        this.triggerListener(new DataSynchronizerCallback<Void, Void>() {
            @Override
            public void onCompletion(Throwable error, Void key, Void result) {
                if (error != null) {
                    log.error("Failed to persist offsets to storage: {}", error);
                } else {
                    log.trace("Successed to persist offsets to storage: {}", offsets);
                }
            }
        });
    }

    @Override
    public void putPosition(ExtendRecordPartition partition, RecordOffset position) {
        offsetStore.put(partition, position);
        this.triggerListener(new DataSynchronizerCallback<Void, Void>() {
            @Override
            public void onCompletion(Throwable error, Void key, Void result) {
                if (error != null) {
                    log.error("Failed to persist offsets to storage: {}", error);
                } else {
                    log.trace("Successed to persist offsets to storage: {}, {}", partition, position);
                }
            }
        });
    }

    @Override
    public void removePosition(List<ExtendRecordPartition> offsets) {
        if (null == offsets) {
            return;
        }
        for (ExtendRecordPartition offset : offsets) {
            offsetStore.remove(offset);
        }
        this.triggerListener(new DataSynchronizerCallback<Void, Void>() {
            @Override
            public void onCompletion(Throwable error, Void key, Void result) {
                if (error != null) {
                    log.error("Failed to persist offsets to storage: {}", error);
                } else {
                    log.trace("Successed to persist offsets to storage: {}", offsets);
                }
            }
        });
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {
        this.offsetUpdateListener = listener;
    }


    private Future<Void> triggerListener(DataSynchronizerCallback<Void, Void> callback) {
        if (offsetUpdateListener != null) {
            offsetUpdateListener.onPositionUpdate();
        }

        return executor.submit(new Callable<Void>() {
            /**
             * Computes a result, or throws an exception if unable to do so.
             *
             * @return computed result
             * @throws Exception if unable to compute a result
             */
            @Override
            public Void call() {
                try {
                    offsetStore.persist();
                    if (callback != null) {
                        callback.onCompletion(null, null, null);
                    }
                } catch (Exception error) {
                    callback.onCompletion(error, null, null);
                }
                return null;
            }
        });
    }

}

