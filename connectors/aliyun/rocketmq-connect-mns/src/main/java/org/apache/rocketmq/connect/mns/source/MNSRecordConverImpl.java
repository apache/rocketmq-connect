package org.apache.rocketmq.connect.mns.source;

import com.aliyun.mns.model.Message;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.SchemaBuilder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

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
        Map<String, Object> mnsDataMap = new HashMap<>();
        mnsDataMap.put("requestId", popMsg.getRequestId());
        mnsDataMap.put("messageId", popMsg.getMessageId());
        mnsDataMap.put("messageBody", messageBody);
        connectRecord.setData(new Gson().toJson(mnsDataMap));
        connectRecord.setSchema(SchemaBuilder.string().build());
    }

    private JsonElement parseToJsonElement(String messageBodyValue) {
        try {
            return JsonParser.parseString(messageBodyValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
