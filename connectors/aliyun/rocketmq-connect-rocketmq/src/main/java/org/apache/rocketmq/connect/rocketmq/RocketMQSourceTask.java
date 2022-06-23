package org.apache.rocketmq.connect.rocketmq;

import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.shade.com.google.common.collect.Maps;
import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class RocketMQSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSourceTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String namesrvAddr;

    private String topic;

    private String instanceId;

    private String consumerGroup;

    private Consumer consumer;

    private final BlockingQueue<ConnectRecord> blockingQueue = new LinkedBlockingDeque<>(1000);
    private static final int BATCH_POLL_SIZE = 10;
    private static final int DEFAULT_CONSUMER_TIMEOUT_SECONDS = 20;

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<ConnectRecord> connectRecords = Lists.newArrayList();
        blockingQueue.drainTo(connectRecords, BATCH_POLL_SIZE);
        return connectRecords;
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void validate(KeyValue config) {
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(RocketMQConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(RocketMQConstant.ACCESS_KEY_SECRET);
        namesrvAddr = config.getString(RocketMQConstant.NAMESRV_ADDR);
        topic = config.getString(RocketMQConstant.TOPIC);
        instanceId = config.getString(RocketMQConstant.INSTANCE_ID);
        consumerGroup = config.getString(RocketMQConstant.CONSUMER_GROUP);
    }

    @Override
    public void start(SourceTaskContext sourceTaskContext) {
        super.start(sourceTaskContext);
    }

    private void initConsumer(String tag) {
        try {
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.GROUP_ID, consumerGroup);
            properties.put(PropertyKeyConst.AccessKey, accessKeyId);
            properties.put(PropertyKeyConst.SecretKey, accessKeySecret);
            properties.put(PropertyKeyConst.NAMESRV_ADDR, namesrvAddr);
            if (StringUtils.isNotBlank(instanceId)) {
                properties.put(PropertyKeyConst.INSTANCE_ID, instanceId);
            }
            consumer = ONSFactory.createConsumer(properties);
            consumer.subscribe(topic, tag, (message, consumeContext) -> {
                try {
                    log.info("RocketMQSourceTask | commit | initConsumer | message  : {}", message);
                    Map<String, String> sourceRecordPartition = Maps.newHashMap();
                    sourceRecordPartition.put("topic", message.getTopic());
                    sourceRecordPartition.put("brokerName", message.getBornHost());
                    Map<String, String> sourceRecordOffset = Maps.newHashMap();
                    sourceRecordOffset.put("queueOffset", Long.toString(message.getOffset()));
                    RecordPartition recordPartition = new RecordPartition(sourceRecordPartition);
                    RecordOffset recordOffset = new RecordOffset(sourceRecordOffset);
                    ConnectRecord connectRecord = new ConnectRecord(recordPartition, recordOffset, message.getBornTimestamp());
                    connectRecord.setData(new String(message.getBody(), StandardCharsets.UTF_8));
                    final Properties userProperties = message.getUserProperties();
                    final Set<String> keys = userProperties.stringPropertyNames();
                    keys.forEach(key -> connectRecord.addExtension(key, userProperties.get(key).toString()));
                    final boolean offer = blockingQueue.offer(connectRecord, DEFAULT_CONSUMER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    if (!offer) {
                        return Action.ReconsumeLater;
                    }
                    return Action.CommitMessage;
                } catch (Exception e) {
                    log.error("RocketMQSourceTask | initConsumer | error => ", e);
                    return Action.ReconsumeLater;
                }
            });
        } catch (Exception e) {
            log.error("RocketMQSourceTask | initConsumer | error => ", e);
            throw e;
        }
    }

    @Override
    public void commit(List<ConnectRecord> connectRecords) throws InterruptedException {
        try {
            if (connectRecords.isEmpty()) return;
            final ConnectRecord connectRecord = connectRecords.get(0);
            initConsumer(connectRecord.getExtension(RocketMQConstant.TAG));
            consumer.start();
        } catch (Exception e) {
            log.error("RocketMQSourceTask | commit | error => ", e);
            throw e;
        }
    }

    @Override
    public void stop() {
        consumer.shutdown();
    }
}
