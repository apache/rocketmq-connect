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

package org.apache.rocketmq.connect.hologres.config;

import io.openmessaging.KeyValue;

import static org.apache.rocketmq.connect.hologres.config.HologresConstant.DYNAMIC_PARTITION;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.DYNAMIC_PARTITION_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.WRITE_MODE;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.WRITE_MODE_DEFAULT;

public class HologresSinkConfig extends AbstractHologresConfig {

    private boolean dynamicPartition;
    private String writeMode;

    public HologresSinkConfig(KeyValue keyValue) {
        super(keyValue);
        this.dynamicPartition = Boolean.parseBoolean(keyValue.getString(DYNAMIC_PARTITION, DYNAMIC_PARTITION_DEFAULT));
        this.writeMode = keyValue.getString(WRITE_MODE, WRITE_MODE_DEFAULT);
    }

    public boolean isDynamicPartition() {
        return dynamicPartition;
    }

    public void setDynamicPartition(boolean dynamicPartition) {
        this.dynamicPartition = dynamicPartition;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }
}
