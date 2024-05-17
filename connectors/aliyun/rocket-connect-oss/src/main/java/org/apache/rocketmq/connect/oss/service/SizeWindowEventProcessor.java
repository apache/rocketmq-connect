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
import java.util.concurrent.atomic.AtomicLong;

public class SizeWindowEventProcessor implements EventProcessor{
    private static final Logger logger = LoggerFactory.getLogger(SizeWindowEventProcessor.class);

    private static AtomicLong batchSize;

    private static Long MAX_BATCH_SIZE;

    private final TaskConfig taskConfig;
    private final OSSClientProvider ossClient;

    private final String dtmPattern;

    private Boolean isStarted;

    private final ConcurrentLinkedQueue<ConnectRecord> queue;

    public SizeWindowEventProcessor(TaskConfig config) {
        this.taskConfig = config;
        this.ossClient = new OSSClientProvider(config);
        if (config.getPartitionType().equals(TaskConstants.PARTITION_TYPE_DEFAULT)) {
            dtmPattern = "dtm={DTM}/hh={HH}/";
        } else {
            dtmPattern = config.getPartitionType();
        }
        if (config.getBatchSendType().equals(TaskConstants.SIZE_WINDOW_BATCH_SEND)) {
            MAX_BATCH_SIZE = ConfigConvertUtils.extractBatchSize(config.getBatchSize());
            batchSize = new AtomicLong(0);
        }
        queue = new ConcurrentLinkedQueue<>();
    }
    @Override
    public CloudEventSendResult send(ConnectRecord connectRecord) {
        int recordLength = connectRecord.getData().toString().getBytes().length;
        if (batchSize.get() + recordLength > MAX_BATCH_SIZE) {
            asyncSendBatch();
            batchSize.set(0);
        }
        queue.offer(connectRecord);
        batchSize.addAndGet(recordLength);
        return new CloudEventSendResult(CloudEventSendResult.ResultStatus.PENDING,String.format("Key:%s", connectRecord.getKey()),null);
    }

    /**
     * 批量将CloudEvent事件存入OSS中
     */
    public void asyncSendBatch() {
        logger.info("The cumulative messages exceeded {} MB,need to send batch messages to OSSBucket:{}",
                MAX_BATCH_SIZE / 1024 / 1024,taskConfig.getBucketName());
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
