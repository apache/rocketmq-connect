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

package org.apache.rocketmq.connect.runtime.connectorwrapper.status;

/**
 * connector state
 */
public class ConnectorStatus extends AbstractStatus<String> {

    public ConnectorStatus() {
    }

    public ConnectorStatus(String id, State state, String workerId, Long generation) {
        super(id, state, workerId, generation, null);
    }

    public ConnectorStatus(String id, State state, String workerId, Long generation, String trace) {
        super(id, state, workerId, generation, null);
    }

    public interface Listener {

        /**
         * Invoked after connector has successfully been shutdown.
         *
         * @param connector The connector name
         */
        void onShutdown(String connector);

        /**
         * error
         *
         * @param connector
         * @param cause
         */
        void onFailure(String connector, Throwable cause);

        /**
         * Invoked when the connector is paused through the REST API
         *
         * @param connector The connector name
         */
        void onPause(String connector);

        /**
         * Invoked after the connector has been resumed.
         *
         * @param connector The connector name
         */
        void onResume(String connector);

        /**
         * Invoked after successful startup of the connector.
         *
         * @param connector The connector name
         */
        void onStartup(String connector);

        /**
         * Invoked when the connector is deleted through the REST API.
         *
         * @param connector The connector name
         */
        void onDeletion(String connector);

    }

}
