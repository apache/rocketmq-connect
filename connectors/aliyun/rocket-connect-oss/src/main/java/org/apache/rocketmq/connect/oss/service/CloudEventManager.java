package org.apache.rocketmq.connect.oss.service;

import com.google.common.base.Preconditions;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.oss.common.constants.TaskConstants;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.exception.LifeCycleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloudEventManager implements LifeCycle{

    private static final Logger logger = LoggerFactory.getLogger(CloudEventManager.class);

    private Boolean isStarted;

    private TaskConfig taskConfig;
    private EventProcessor eventProcessor;

    public CloudEventManager(KeyValue keyValue) {
        validate(keyValue);
        this.taskConfig = new TaskConfig();
        taskConfig.load(keyValue);
        switch (taskConfig.getBatchSendType()) {
            case TaskConstants.PARTITION_TYPE_DEFAULT:
                eventProcessor = new DefaultEventProcessor(taskConfig);
                break;
            case TaskConstants.TIME_WINDOW_BATCH_SEND:
                eventProcessor = new TimeWindowEventProcessor(taskConfig);
                break;
            case TaskConstants.SIZE_WINDOW_BATCH_SEND:
                eventProcessor = new SizeWindowEventProcessor(taskConfig);
                break;
        }
        this.start();
    }

    @Override
    public void start() throws LifeCycleException {
        logger.info("CloudEventManager is starting...");
        isStarted = true;
        eventProcessor.start();
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }

    @Override
    public void shutdown() {
        logger.info("CloudEventManager is shutting down...");
        this.taskConfig = null;
        eventProcessor.shutdown();
    }

    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(TaskConfig.ENDPOINT))
                || StringUtils.isBlank(config.getString(TaskConfig.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(TaskConfig.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(TaskConfig.BUCKET_NAME))) {
            throw new RuntimeException("OSS client parameters is blank!");
        }
        // 校验是否需要自定义分区
        if (StringUtils.isBlank(config.getString(TaskConfig.PARTITION_TYPE))) {
            config.put(TaskConfig.PARTITION_TYPE,TaskConstants.PARTITION_TYPE_DEFAULT);
        }
        // 校验是否有聚批功能
        if (StringUtils.isBlank(config.getString(TaskConfig.BATCH_SEND_TYPE))) {
            config.put(TaskConfig.BATCH_SEND_TYPE,TaskConstants.BATCH_SEND_TYPE_DEFAULT);
        }
        // 校验聚批功能相关参数
        if (StringUtils.equals(config.getString(TaskConfig.BATCH_SEND_TYPE), TaskConstants.TIME_WINDOW_BATCH_SEND)) {
            Preconditions.checkArgument(!StringUtils.isBlank(config.getString(TaskConfig.BATCH_SEND_TIME_INTERVAL)));
        }
        if (StringUtils.equals(config.getString(TaskConfig.BATCH_SEND_TYPE), TaskConstants.SIZE_WINDOW_BATCH_SEND)) {
            Preconditions.checkArgument(!StringUtils.isBlank(config.getString(TaskConfig.BATCH_SEND_GROUP_SIZE)));
        }
    }

    public void send(ConnectRecord connectRecord) {
        eventProcessor.send(connectRecord);
    }
}
