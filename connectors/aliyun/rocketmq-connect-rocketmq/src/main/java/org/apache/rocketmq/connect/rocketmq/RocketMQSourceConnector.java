package org.apache.rocketmq.connect.rocketmq;

import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;

import java.util.ArrayList;
import java.util.List;

public class RocketMQSourceConnector extends SourceConnector {

    private String accessKeyId;

    private String accessKeySecret;

    private String namesrvAddr;

    private String topic;

    private String instanceId;

    private String consumerGroup;

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
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET,accessKeySecret);
        keyValue.put(RocketMQConstant.INSTANCE_ID, instanceId);
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, namesrvAddr);
        keyValue.put(RocketMQConstant.TOPIC, topic);
        keyValue.put(RocketMQConstant.CONSUMER_GROUP, consumerGroup);
        keyValues.add(keyValue);
        return keyValues;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return RocketMQSourceTask.class;
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
    public void stop() {

    }
}
