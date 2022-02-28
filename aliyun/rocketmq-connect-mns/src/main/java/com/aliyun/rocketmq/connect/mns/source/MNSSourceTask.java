package com.aliyun.rocketmq.connect.mns.source;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ClientException;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.model.Message;
import com.aliyun.rocketmq.connect.mns.source.utils.AliyunMnsUtil;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MNSSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MNSSourceTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String queueName;

    private String accountId;

    private String isBase64Decode;

    private MNSClient mnsClient;

    private CloudQueue cloudQueue;

    private Integer batchSize;

    private AbstractMNSRecordConvert abstractMNSRecordConvert;

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<ConnectRecord> result = new ArrayList<>(11);
        try{
            List<Message> messageList = cloudQueue.batchPopMessage(batchSize);
            if (messageList != null && !messageList.isEmpty()) {
                messageList.forEach(message -> {
                    result.addAll(this.abstractMNSRecordConvert.toConnectRecord(
                            AliyunMnsUtil.parseRegionIdFromEndpoint(accountEndpoint), accountId, queueName, message, Boolean.parseBoolean(isBase64Decode)));
                });
            }
        } catch (ClientException ce) {
            log.error("Something wrong with the network connection between client and MNS service."
                    + "Please check your network and DNS availability.", ce);
        } catch (ServiceException se) {
            log.error("MNS exception requestId: " + se.getRequestId(), se);
            if (se.getErrorCode().equals("QueueNotExist")) {
                log.error("Queue is not exist.Please create before use");
            } else if (se.getErrorCode().equals("TimeExpired")) {
                log.error("The request is time expired. Please check your local machine timeclock");
            }
        } catch (Exception e) {
            log.error("Unknown exception happened! ", e);
        }
        return result;
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString("MNSAccessKeyId"))
                || StringUtils.isBlank(config.getString("MNSAccessKeySecret"))
                || StringUtils.isBlank(config.getString("MNSAccountEndpoint"))
                || StringUtils.isBlank(config.getString("queueName"))) {
            throw new RuntimeException("mns required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString("accessKeyId");
        accessKeySecret = config.getString("accessKeySecret");
        accountEndpoint = config.getString("accountEndpoint");
        queueName = config.getString("queueName");
        batchSize = config.getInt("batchSize", 8);
        accountId = config.getString("accountId");
        isBase64Decode = config.getString("isBase64Decode", "true");
        abstractMNSRecordConvert = new MNSRecordConverImpl();
    }

    @Override
    public void commit(final List<ConnectRecord> connectRecords) throws InterruptedException {
        if (connectRecords == null || connectRecords.isEmpty()) {
            return;
        }
        Set<String> receiptHandlesSet = new HashSet<>(connectRecords.size());
        try {
            connectRecords.forEach(connectRecord -> receiptHandlesSet.add(connectRecord.getExtension(AbstractMNSRecordConvert.KEY_RECEIPT_HANDLE)));
            List<String> receiptHandles = new ArrayList<>(receiptHandlesSet.size());
            receiptHandles.addAll(receiptHandlesSet);
            cloudQueue.batchDeleteMessage(receiptHandles);
        } catch (Exception e) {
            log.error("MNSSourceTask | commit | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit() {
        super.commit();
    }

    @Override
    public void start(SourceTaskContext sourceTaskContext) {
        super.start(sourceTaskContext);
        try {
            CloudAccount cloudAccount = new CloudAccount(accessKeyId, accessKeySecret, accountEndpoint);
            mnsClient = cloudAccount.getMNSClient();
            cloudQueue = mnsClient.getQueueRef(queueName);
        } catch (Exception e) {
            log.error("MNSSourceTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        mnsClient.close();
    }
}
