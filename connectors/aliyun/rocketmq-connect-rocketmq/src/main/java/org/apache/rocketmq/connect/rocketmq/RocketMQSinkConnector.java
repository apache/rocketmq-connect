package org.apache.rocketmq.connect.rocketmq;


import com.aliyun.ons20190214.Client;
import com.aliyun.ons20190214.models.OnsTopicListRequest;
import com.aliyun.ons20190214.models.OnsTopicListResponse;
import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import com.aliyun.teaopenapi.models.Config;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.apache.rocketmq.connect.rocketmq.utils.OnsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RocketMQSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSinkConnector.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String namesrvAddr;

    private String topic;

    private String instanceId;

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValues = new ArrayList<>();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(RocketMQConstant.ACCESS_KEY_ID, accessKeyId);
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(RocketMQConstant.TOPIC, topic);
        keyValue.put(RocketMQConstant.INSTANCE_ID, instanceId);
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, namesrvAddr);
        keyValues.add(keyValue);
        return keyValues;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RocketMQSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(RocketMQConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(RocketMQConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(RocketMQConstant.NAMESRV_ADDR))
                || StringUtils.isBlank(config.getString(RocketMQConstant.TOPIC))) {
            throw new RuntimeException("rocketmq required parameter is null !");
        }
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
    }

    @Override
    public void stop() {

    }
}
