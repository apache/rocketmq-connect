package org.apache.rocketmq.connect.oss.service;

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.oss.common.cloudEvent.CloudEventSendResult;
import org.apache.rocketmq.connect.oss.common.constants.TaskConstants;
import org.apache.rocketmq.connect.oss.common.oss.OSSClientProvider;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.exception.LifeCycleException;
import org.apache.rocketmq.connect.oss.util.ConfigConvertUtils;
import org.apache.rocketmq.connect.oss.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TimeWindowEventProcessor implements EventProcessor{

    private static final Logger logger = LoggerFactory.getLogger(TimeWindowEventProcessor.class);

    private Boolean isStarted;
    private TaskConfig taskConfig;
    private final OSSClientProvider ossClient;

    private static ScheduledExecutorService scheduler;

    private final String dtmPattern;

    private final ConcurrentLinkedQueue<ConnectRecord> queue;

    public TimeWindowEventProcessor(TaskConfig config) {
        this.taskConfig = config;
        this.ossClient = new OSSClientProvider(config);
        if (config.getPartitionType().equals(TaskConstants.PARTITION_TYPE_DEFAULT)) {
            dtmPattern = "dtm={DTM}/hh={HH}/";
        } else {
            dtmPattern = config.getPartitionType();
        }
        queue = new ConcurrentLinkedQueue<>();
    }


    @Override
    public CloudEventSendResult send(ConnectRecord connectRecord) {
        logger.info("record has been saved in TimeWindowEventProcessor, recordKey:{}",connectRecord.getKey());
        queue.offer(connectRecord);
        return new CloudEventSendResult(CloudEventSendResult.ResultStatus.PENDING,String.format("Key:%s", connectRecord.getKey()),null);
    }

    /**
     * 批量将CloudEvent事件存入OSS中
     */
    public void asyncSendBatch() {
        if (queue.isEmpty()) {
            logger.info("TimeWindowEventProcessor has no msg to send");
            return;
        }
        while(queue.peek() != null) {
            ConnectRecord connectRecord = queue.poll();
            try {
                String timePartition = TimeUtils.generateDtm(connectRecord.getTimestamp(), dtmPattern);
                String key = timePartition + connectRecord.getKey();
                ossClient.putObject(taskConfig.getBucketName(), key, connectRecord.getData().toString());
                logger.info("record has been sent to OSS,OSS bucket:{}, recordKey:{}", taskConfig.getBucketName(), connectRecord.getKey());
            } catch (Exception e) {
                logger.error("record sent to OSS throw Exception,recordKey:{}, error:{}",connectRecord.getKey(), e.toString());
            }
        }
    }



    @Override
    public void start() throws LifeCycleException {
        scheduler = Executors.newScheduledThreadPool(1);
        ConfigConvertUtils.TimeValue timeInterval = ConfigConvertUtils.extractTimeInterval(taskConfig.getTimeInterval());
        scheduler.scheduleAtFixedRate(this::asyncSendBatch, 0, timeInterval.getValue(), timeInterval.getUnit());
        isStarted = true;
    }

    @Override
    public boolean isStarted() {
        return isStarted;
    }

    @Override
    public void shutdown() {
        scheduler.shutdown();
        ossClient.stop();
        taskConfig = null;
        isStarted = false;
    }
}
