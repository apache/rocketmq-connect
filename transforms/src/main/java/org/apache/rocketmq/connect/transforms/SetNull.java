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

/**
 * set null
 *
 * @param <R>
 */
public abstract class SetNull<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(SetNull.class);

    PatternRenameConfig config;

    @Override
    public void start(KeyValue keyValue) {
        config = new PatternRenameConfig(keyValue);
    }

    /**
     * transform key
     */
    public static class Key extends SetNull<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            return new ConnectRecord(
                r.getPosition().getPartition(),
                r.getPosition().getOffset(),
                r.getTimestamp(),
                null,
                null,
                r.getSchema(),
                r.getData()
            );
        }
    }

    public static class Value extends SetNull<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            return new ConnectRecord(
                r.getPosition().getPartition(),
                r.getPosition().getOffset(),
                r.getTimestamp(),
                r.getKeySchema(),
                r.getKey(),
                r.getSchema(),
                r.getData()
            );
        }
    }
}
