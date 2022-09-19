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
 */

package org.apache.rocketmq.connect.file;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterTransform implements Transform<ConnectRecord> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILE_CONNECTOR);

    private KeyValue keyValue;

    @Override public ConnectRecord doTransform(ConnectRecord record) {
        Object data = record.getData();
        String s = String.valueOf(data);
        s = s + ":filter";
        record.setData(s);
        return record;
    }


    /**
     * Start the component
     *
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        this.keyValue = config;
        log.info("transform config {}", this.keyValue);
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {

    }
}
