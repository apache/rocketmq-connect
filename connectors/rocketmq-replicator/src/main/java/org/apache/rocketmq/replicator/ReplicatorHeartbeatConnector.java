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
package org.apache.rocketmq.replicator;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig;
import org.apache.rocketmq.replicator.utils.ReplicatorUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig.CONNECT_TOPICNAME;


/**
 * @author osgoo
 * @date 2022/6/16
 */
public class ReplicatorHeartbeatConnector extends SourceConnector {
    private Log log = LogFactory.getLog(ReplicatorHeartbeatConnector.class);
    private KeyValue config;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        // use SRC_TOPICTAGS for heartbeat topic
        String srcInstanceId = this.config.getString(ReplicatorConnectorConfig.SRC_INSTANCEID, "");
        String srcTopic = this.config.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS, ReplicatorConnectorConfig.DEFAULT_HEARTBEAT_TOPIC);
        srcTopic = ReplicatorUtils.buildTopicWithNamespace(srcTopic, srcInstanceId);
        this.config.put(ReplicatorConnectorConfig.HEARTBEAT_TOPIC, srcTopic);
        List<KeyValue> config = new ArrayList<>();
        config.add(this.config);
        return config;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ReplicatorHeartbeatTask.class;
    }

    private Map<String, Boolean> neededParamKeys = new HashMap<String, Boolean>() {
        {
            put(ReplicatorConnectorConfig.SRC_CLOUD, false);
            put(ReplicatorConnectorConfig.SRC_REGION, false);
            put(ReplicatorConnectorConfig.SRC_CLUSTER, false);
            put(ReplicatorConnectorConfig.SRC_INSTANCEID, false);
            put(ReplicatorConnectorConfig.SRC_ENDPOINT, true);
            put(ReplicatorConnectorConfig.SRC_TOPICTAGS, true);
            put(ReplicatorConnectorConfig.DEST_CLOUD, false);
            put(ReplicatorConnectorConfig.DEST_REGION, false);
            put(ReplicatorConnectorConfig.DEST_CLUSTER, false);
            put(ReplicatorConnectorConfig.DEST_INSTANCEID, false);
            put(ReplicatorConnectorConfig.DEST_ENDPOINT, true);
            put(ReplicatorConnectorConfig.DEST_TOPIC, true);
            put(ReplicatorConnectorConfig.SRC_CLOUD, false);
            put(ReplicatorConnectorConfig.SRC_ACL_ENABLE, false);
            put(ReplicatorConnectorConfig.DEST_ACL_ENABLE, false);
        }
    };


    @Override
    public void validate(KeyValue config) {
        if (config.getInt(ConnectorConfig.MAX_TASK) > 1) {
            log.warn("ReplicatorHeartbeatConnector no need to set max-task, only used 1.");
        }
        // heartbeat just need only one task.
        config.put(ConnectorConfig.MAX_TASK, 1);
        if (StringUtils.isNotBlank(config.getString(CONNECT_TOPICNAME))) {
            log.warn("ReplicatorHeartbeatConnector no need to set " + CONNECT_TOPICNAME + ", use " + ReplicatorConnectorConfig.DEST_TOPIC + " instead.");
            // use destInstanceId % destTopic for sink instead of CONNECT_TOPICNAME
            config.put(CONNECT_TOPICNAME, "");
        }

        ReplicatorUtils.checkNeedParams(ReplicatorHeartbeatConnector.class.getName(), config, neededParamKeys);

    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = keyValue;
    }

    @Override
    public void stop() {

    }
}
