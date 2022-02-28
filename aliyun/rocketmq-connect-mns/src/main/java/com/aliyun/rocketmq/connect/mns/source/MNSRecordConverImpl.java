package com.aliyun.rocketmq.connect.mns.source;

import com.aliyun.mns.model.Message;
import com.aliyun.rocketmq.connect.mns.source.enums.CloudEventsEnum;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class MNSRecordConverImpl extends AbstractMNSRecordConvert {


    @Override
    public void fillCloudEventsKey(ConnectRecord connectRecord, String regionId, String accountId, String queueName, Message popMsg, boolean isBase64Secode) {
        String messageBodyValue = "";
        if (isBase64Secode) {
            messageBodyValue = new String(popMsg.getMessageBodyAsBytes(), StandardCharsets.UTF_8);
        } else {
            messageBodyValue = new String(popMsg.getMessageBodyAsRawBytes(), StandardCharsets.UTF_8);
        }
        JsonElement messageBody = null;
        try {
            messageBody = parseToJsonElement(messageBodyValue);
        } catch (Exception e) {
            messageBody = new JsonPrimitive(messageBodyValue);
        }
        String eventId;
        if (StringUtils.isBlank(popMsg.getMessageId())) {
            eventId = popMsg.getMessageId();
        } else {
            eventId = UUID.randomUUID().toString();
        }
        connectRecord.addExtension(CloudEventsEnum.CE_ID.getCode(), eventId);
        connectRecord.addExtension(CloudEventsEnum.CE_SOURCE.getCode(), ACS_MNS);
        connectRecord.addExtension(CloudEventsEnum.CE_SPECVERSION.getCode(), CloudEventsEnum.CE_SPECVERSION.getDefaultValue());
        connectRecord.addExtension(CloudEventsEnum.CE_TYPE.getCode(), MNS_QUEUE_SEND_MESSAGE);
        connectRecord.addExtension(CloudEventsEnum.CE_DATACONTENTTYPE.getCode(), CloudEventsEnum.CE_SPECVERSION.getDefaultValue());
        connectRecord.addExtension(CloudEventsEnum.CE_TYPE.getCode(), Instant.ofEpochMilli(popMsg.getEnqueueTime().getTime()).toString());
        connectRecord.addExtension(CloudEventsEnum.CE_SUBJECT.getCode(), buildSubject(regionId, accountId, queueName));
        connectRecord.addExtension(CloudEventsEnum.CE_ALIYUNACCOUNTID.getCode(), accountId);
        Map<String, Object> mnsDataMap = new HashMap<>();
        mnsDataMap.put("requestId", popMsg.getRequestId());
        mnsDataMap.put("messageId", popMsg.getMessageId());
        mnsDataMap.put("messageBody", messageBody);
        connectRecord.setData(new Gson().toJson(mnsDataMap).getBytes(StandardCharsets.UTF_8));
        connectRecord.setSchema(SchemaBuilder.bytes().build());
    }

    private String buildSubject(String regionId, String accountId, String queueName) {
        return new StringBuilder().append("acs:mns:").append(regionId).append(":").append(accountId).append(":").append("queues").append("/").append(queueName).toString();
    }

    private JsonElement parseToJsonElement(String messageBodyValue) {
        try {
            return JsonParser.parseString(messageBodyValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
