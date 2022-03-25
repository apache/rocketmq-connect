package org.apache.rocketmq.connect.rocketmq;

import com.aliyun.ons20190214.Client;
import com.aliyun.ons20190214.models.OnsTopicListRequest;
import com.aliyun.ons20190214.models.OnsTopicListResponse;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.shade.com.alibaba.fastjson.JSON;
import com.aliyun.openservices.shade.org.apache.commons.lang3.StringUtils;
import com.aliyun.teaopenapi.models.Config;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.apache.rocketmq.connect.rocketmq.utils.OnsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class RocketMQSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(RocketMQSinkTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String namesrvAddr;

    private String topic;

    private Producer producer;

    private String instanceId;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(connectRecord -> {
                Message message = new Message();
                message.setBody(JSON.toJSONString(connectRecord.getData()).getBytes(StandardCharsets.UTF_8));
                // TODO message.setKey();
                // TODO message.setTag();
                final KeyValue extensions = connectRecord.getExtensions();
                if (extensions != null) {
                    extensions.keySet().forEach(key -> message.putUserProperties(key, extensions.getString(key)));
                }
                message.setTopic(topic);
                producer.send(message);
            });
        } catch (Exception e) {
            log.error("RocketMQSinkTask | put | error => ", e);
            throw new ConnectException(e);
        }
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
            || StringUtils.isBlank(config.getString(RocketMQConstant.TOPIC))) {
            throw new RuntimeException("rocketmq required parameter is null !");
        }
        // 检查topic是否存在
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
    public void start(SinkTaskContext sinkTaskContext) {
        try {
            super.start(sinkTaskContext);
            if (producer != null) {
                producer.shutdown();
            }
            Properties properties = new Properties();
            properties.put(PropertyKeyConst.AccessKey, accessKeyId);
            properties.put(PropertyKeyConst.SecretKey, accessKeySecret);
            if (StringUtils.isNotBlank(instanceId)) {
                properties.put(PropertyKeyConst.INSTANCE_ID,  instanceId);
            }
            properties.put(PropertyKeyConst.NAMESRV_ADDR, namesrvAddr);
            producer = ONSFactory.createProducer(properties);
            producer.start();
        } catch (Exception e) {
            log.error("RocketMQSinkTask | start | error =>", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        producer.shutdown();
    }
}
