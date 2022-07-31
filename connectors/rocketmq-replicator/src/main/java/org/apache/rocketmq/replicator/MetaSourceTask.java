/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.replicator;

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.replicator.common.Utils;
import org.apache.rocketmq.replicator.config.ConfigUtil;
import org.apache.rocketmq.replicator.config.TaskConfig;
import org.apache.rocketmq.replicator.offset.OffsetSyncStore;
import org.apache.rocketmq.replicator.schema.FieldName;
import org.apache.rocketmq.replicator.schema.SchemaEnum;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetaSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RmqSourceTask.class);

    private final String taskId;
    private final TaskConfig config;
    private DefaultMQAdminExt srcMQAdminExt;
    private volatile boolean started = false;

    private OffsetSyncStore store;

    public MetaSourceTask() {
        this.config = new TaskConfig();
        this.taskId = Utils.createTaskId(Thread.currentThread().getName());
    }

    @Override
    public void start(KeyValue config) {
        ConfigUtil.load(config, this.config);
        try {
            this.srcMQAdminExt = Utils.startMQAdminTool(this.config);
        } catch (MQClientException e) {
            log.error("Replicator task start failed for `startMQAdminTool` exception.", e);
            throw new IllegalStateException("Replicator task start failed for `startMQAdminTool` exception.");
        }

        this.store = new OffsetSyncStore(this.srcMQAdminExt, this.config);
        this.started = true;
    }

    @Override
    public void stop() {
        if (started) {
            started = false;
        }
        srcMQAdminExt.shutdown();
    }

    @Override public List<ConnectRecord> poll() {
        log.debug("polling...");
        List<String> groups = JSONObject.parseArray(this.config.getTaskGroupList(), String.class);

        if (groups == null) {
            log.info("no group in task.");
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
            return Collections.emptyList();
        }
        List<ConnectRecord> res = new ArrayList<>();
        for (String group : groups) {
            ConsumeStats stats;
            try {
                stats = this.srcMQAdminExt.examineConsumeStats(group);
            } catch (Exception e) {
                log.error("admin get consumer info failed for consumer groups: " + group, e);
                continue;
            }

            for (Map.Entry<MessageQueue, OffsetWrapper> offsetTable : stats.getOffsetTable().entrySet()) {
                MessageQueue mq = offsetTable.getKey();
                long srcOffset = offsetTable.getValue().getConsumerOffset();
                long targetOffset = this.store.convertTargetOffset(mq, group, srcOffset);

                List<Field> fields = new ArrayList<Field>();
                Schema schema = new Schema(SchemaEnum.OFFSET.name(), FieldType.INT64, fields);
                schema.getFields().add(new Field(0, FieldName.OFFSET.getKey(), SchemaBuilder.string().build()));

                JSONObject jsonObject = new JSONObject();
                jsonObject.put(FieldName.OFFSET.getKey(), targetOffset);
                ConnectRecord connectRecord = new ConnectRecord(Utils.offsetKey(mq),
                    Utils.offsetValue(srcOffset), System.currentTimeMillis(), schema, jsonObject.toJSONString());
                res.add(connectRecord);
            }
        }
        return res;
    }
}
