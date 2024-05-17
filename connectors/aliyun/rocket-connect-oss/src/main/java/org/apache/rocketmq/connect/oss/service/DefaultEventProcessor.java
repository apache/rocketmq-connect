package org.apache.rocketmq.connect.oss.service;

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.oss.common.cloudEvent.CloudEventSendResult;
import org.apache.rocketmq.connect.oss.common.constants.TaskConstants;
import org.apache.rocketmq.connect.oss.common.oss.OSSClientProvider;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.exception.LifeCycleException;
import org.apache.rocketmq.connect.oss.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultEventProcessor implements EventProcessor{

    private static final Logger logger = LoggerFactory.getLogger(DefaultEventProcessor.class);

    private final OSSClientProvider ossClient;

    private final TaskConfig taskConfig;

    private final String dtmPattern;

    private Boolean isStarted;


    public DefaultEventProcessor(TaskConfig config) {
        this.taskConfig = config;
        this.ossClient = new OSSClientProvider(taskConfig);
        if (config.getPartitionType().equals(TaskConstants.PARTITION_TYPE_DEFAULT)) {
            dtmPattern = "dtm={DTM}/hh={HH}/";
        } else {
            dtmPattern = config.getPartitionType();
        }
    }
    @Override
    public CloudEventSendResult send(ConnectRecord connectRecord) {
        logger.info("record has been saved in DefaultEventProcessor, recordKey:{}",connectRecord.getKey());
        String timePartition = TimeUtils.generateDtm(connectRecord.getTimestamp(), dtmPattern);
        String key = timePartition + connectRecord.getKey();
        this.ossClient.putObject(taskConfig.getBucketName(), key,
                connectRecord.getData().toString());
        return new CloudEventSendResult(CloudEventSendResult.ResultStatus.SUCCESS, String.format("Key:%s", key), null);
    }


    @Override
    public void start() throws LifeCycleException {
        isStarted = true;
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }

    @Override
    public void shutdown() {
        isStarted = false;
        ossClient.stop();
    }
}
