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

package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import io.openmessaging.KeyValue;

/**
 * kafka source connector
 */
public abstract class KafkaSinkAdaptorConnector extends AbstractKafkaSinkConnector {

    /**
     * Start the component
     *
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        super.start(config);
        sinkConnector.validate(taskConfig);
        sinkConnector.initialize(new KafkaConnectorContext(connectorContext));
        sinkConnector.start(taskConfig);
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {
        if (sinkConnector != null) {
            sinkConnector.stop();
        }
        super.stop();
    }
}
