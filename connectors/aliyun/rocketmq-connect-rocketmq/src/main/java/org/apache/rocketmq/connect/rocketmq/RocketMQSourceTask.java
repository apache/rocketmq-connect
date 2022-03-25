package org.apache.rocketmq.connect.rocketmq;

import com.aliyun.ons20190214.Client;
import com.aliyun.ons20190214.models.OnsGroupListRequest;
import com.aliyun.ons20190214.models.OnsGroupListResponse;
import com.aliyun.ons20190214.models.OnsTopicListRequest;
import com.aliyun.ons20190214.models.OnsTopicListResponse;
import com.aliyun.openservices.ons.api.Action;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.shade.com.google.common.collect.Maps;
import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import com.aliyun.teaopenapi.models.Config;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.apache.rocketmq.connect.rocketmq.utils.OnsUtils;
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
        if (consumer == null) {
            initConsumer();
        }
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
        if (StringUtils.isBlank(config.getString(RocketMQConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(RocketMQConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(RocketMQConstant.NAMESRV_ADDR))
                || StringUtils.isBlank(config.getString(RocketMQConstant.TOPIC))
                || StringUtils.isBlank(config.getString(RocketMQConstant.CONSUMER_GROUP))) {
            throw new RuntimeException("rocketmq required parameter is null !");
        }
        // 检查topic和consumer group是否存在
        try {
            Config onsConfig = new Config()
                    .setAccessKeyId(config.getString(RocketMQConstant.ACCESS_KEY_ID))
                    .setAccessKeySecret(config.getString(RocketMQConstant.ACCESS_KEY_SECRET));
            onsConfig.endpoint = OnsUtils.parseEndpoint(config.getString(RocketMQConstant.NAMESRV_ADDR));
            final Client client = new Client(onsConfig);
            OnsTopicListRequest onsTopicListRequest = new OnsTopicListRequest()
                    .setTopic(config.getString(RocketMQConstant.TOPIC))
                    .setInstanceId(config.getString(RocketMQConstant.INSTANCE_ID));
            final OnsTopicListResponse onsTopicListResponse = client.onsTopicList(onsTopicListRequest);
            if (onsTopicListResponse.getBody().getData().getPublishInfoDo().isEmpty()) {
                throw new RuntimeException("rocketmq required parameter topic does not exist !");
            }
            OnsGroupListRequest onsGroupListRequest = new OnsGroupListRequest()
                    .setInstanceId(config.getString(RocketMQConstant.INSTANCE_ID))
                    .setGroupId(config.getString(RocketMQConstant.CONSUMER_GROUP));
            final OnsGroupListResponse onsGroupListResponse = client.onsGroupList(onsGroupListRequest);
            if (onsGroupListResponse.getBody().getData().getSubscribeInfoDo().isEmpty()) {
                throw new RuntimeException("rocketmq required parameter consumerGroup does not exist !");
            }
        } catch (Exception e) {
            log.error("RocketMQSinkTask | validate | error => ", e);
            throw new RuntimeException(e.getMessage());
        }
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
        try {
            super.start(sourceTaskContext);
            initConsumer();
            consumer.start();
        } catch (Exception e) {
            log.error("RocketMQSourceTask | start | error => ", e);
            throw e;
        }
    }

    private void initConsumer() {
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
            // TODO TAG先忽略
            consumer.subscribe(topic, "*", (message, consumeContext) -> {
                try {
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
    public void stop() {
        consumer.shutdown();
    }
}
