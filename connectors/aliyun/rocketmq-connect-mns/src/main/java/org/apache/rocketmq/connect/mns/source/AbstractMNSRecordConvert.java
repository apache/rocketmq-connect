package org.apache.rocketmq.connect.mns.source;

import com.aliyun.mns.model.Message;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractMNSRecordConvert {

    protected static final String KEY_QUEUE_NAME = "MNS:QueueName";
    protected static final String KEY_RECEIPT_HANDLE = "ReceiptHandle";
    protected static final String KEY_MESSAGE_ID = "MessageId";

    protected static final String ACS_MNS= "acs:mns";
    protected static final String MNS_QUEUE_SEND_MESSAGE = "mns:Queue:SendMessage";

    protected List<ConnectRecord> toConnectRecord(String regionId, String accountId, String queueName, Message popMsg, boolean isBase64Secode) {
        RecordPosition recordPosition = buildRecordPosition(queueName, popMsg);
        ConnectRecord connectRecord = new ConnectRecord(recordPosition.getPartition(), recordPosition.getOffset(), popMsg.getEnqueueTime().getTime());
        connectRecord.addExtension(KEY_RECEIPT_HANDLE, popMsg.getReceiptHandle());
        connectRecord.addExtension(KEY_MESSAGE_ID, popMsg.getMessageId());
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        connectRecordList.add(connectRecord);
        fillCloudEventsKey(connectRecord, regionId, accountId, queueName, popMsg, isBase64Secode);
        return connectRecordList;
    }

    protected abstract void fillCloudEventsKey(ConnectRecord connectRecord, String regionId, String accountId, String queueName, Message popMsg, boolean isBase64Secode);

    protected static RecordPosition buildRecordPosition(String queueName, Message popMsg) {
        Map<String, String> sourcePartiton = new HashMap<>();
        sourcePartiton.put(KEY_QUEUE_NAME, queueName);
        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(KEY_RECEIPT_HANDLE, popMsg.getReceiptHandle());
        RecordPartition recordPartition = new RecordPartition(sourcePartiton);
        RecordOffset recordOffset = new RecordOffset(sourceOffset);
        return new RecordPosition(recordPartition, recordOffset);
    }

}
