package org.apache.rocketmq.connect.rocketmq;


import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;

import java.util.ArrayList;
import java.util.List;

public class RocketMQSinkConnector extends SinkConnector {

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
