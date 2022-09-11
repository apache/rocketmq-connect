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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.rocketmq.connect.runtime.store;


import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.storage.OffsetStorageWriter;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * position storage writer
 */
public class PositionStorageWriter implements OffsetStorageWriter, Closeable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private final String namespace;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private PositionManagementService positionManagementService;
    /**
     * Offset data in Connect format
     */
    private Map<ExtendRecordPartition, RecordOffset> data = new HashMap<>();
    private Map<ExtendRecordPartition, RecordOffset> toFlush = null;

    // Unique ID for each flush request to handle callbacks after timeouts
    private long currentFlushId = 0;

    public PositionStorageWriter(String namespace, PositionManagementService positionManagementService) {
        this.namespace = namespace;
        this.positionManagementService = positionManagementService;
    }

    @Override
    public void writeOffset(RecordPartition partition, RecordOffset position) {
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition(namespace, partition.getPartition());
        data.put(extendRecordPartition, position);
    }

    /**
     * write offsets
     *
     * @param positions positions
     */
    @Override
    public void writeOffset(Map<RecordPartition, RecordOffset> positions) {
        for (Map.Entry<RecordPartition, RecordOffset> offset : positions.entrySet()) {
            writeOffset(offset.getKey(), offset.getValue());
        }
    }


    private boolean isFlushing() {
        return toFlush != null;
    }

    /**
     * begin flush offset
     *
     * @return
     */
    public synchronized boolean beginFlush() {
        if (isFlushing()) {
            throw new ConnectException("PositionStorageWriter is already flushing");
        }
        if (data.isEmpty()) {
            return false;
        }
        this.toFlush = this.data;
        this.data = new HashMap<>();
        return true;
    }

    /**
     * do flush offset
     *
     * @param callback
     */
    public Future doFlush(final DataSynchronizerCallback callback) {
        final long flushId = currentFlushId;
        return sendOffsetFuture(callback, flushId);
    }

    /**
     * Cancel a flush that has been initiated by {@link #beginFlush}.
     */
    public synchronized void cancelFlush() {
        if (isFlushing()) {
            // rollback to inited
            toFlush.putAll(data);
            data = toFlush;
            currentFlushId++;
            toFlush = null;
        }
    }

    private Future<Void> sendOffsetFuture(DataSynchronizerCallback callback, long flushId) {
        FutureTask<Void> futureTask = new FutureTask<Void>(new SendOffsetCallback(callback, flushId));
        executorService.submit(futureTask);
        return futureTask;
    }

    /**
     * Closes this stream and releases any system resources associated
     * with it. If the stream is already closed then invoking this
     * method has no effect.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (executorService != null) {
            executorService.shutdown();
        }
    }


    /**
     * send offset callback
     */
    private class SendOffsetCallback implements Callable<Void> {
        DataSynchronizerCallback callback;
        long flushId;

        public SendOffsetCallback(DataSynchronizerCallback callback, long flushId) {
            this.callback = callback;
            this.flushId = flushId;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Void call() {
            try {
                // has been canceled
                if (flushId != currentFlushId) {
                    return null;
                }
                positionManagementService.putPosition(toFlush);
                log.debug("Submitting {} entries to backing store. The offsets are: {}", data.size(), toFlush);
                positionManagementService.persist();
                positionManagementService.synchronize(true);
                // persist finished
                toFlush = null;
                currentFlushId++;
            } catch (Throwable throwable) {
                // rollback
                cancelFlush();
                this.callback.onCompletion(throwable, null, null);
            }
            return null;
        }
    }

}
