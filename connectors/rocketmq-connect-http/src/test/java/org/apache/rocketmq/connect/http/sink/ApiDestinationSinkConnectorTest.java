package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ApiDestinationSinkConnectorTest {

    private final ApiDestinationSinkConnector apiDestinationSinkConnector = new ApiDestinationSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(apiDestinationSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testBasicAuth() {
        ApiDestinationSinkTask apiDestinationSinkTask = new ApiDestinationSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.API_DESTINATION_NAME, "apiDestinationName-13");
        keyValue.put(HttpConstant.ENDPOINT, "http://localhost:7001");
        apiDestinationSinkTask.init(keyValue);
        apiDestinationSinkTask.start(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override
            public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override
            public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override
            public void pause(List<RecordPartition> partitions) {

            }

            @Override
            public void resume(List<RecordPartition> partitions) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("id", "123");
        keyValue.put(HttpConstant.BODYS_CONSTANT, JSONObject.toJSONString(bodyMap));
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.addExtension(HttpConstant.HTTP_QUERY_VALUE, JSONObject.toJSONString(bodyMap));
        connectRecord.setData(JSONObject.toJSONString(new HashMap<>()));
        connectRecordList.add(connectRecord);
        apiDestinationSinkTask.put(connectRecordList);
    }

    @Test
    public void testApiKeyAuth() {
        ApiDestinationSinkTask apiDestinationSinkTask = new ApiDestinationSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.API_DESTINATION_NAME, "apiDestinationName-14");
        keyValue.put(HttpConstant.ENDPOINT, "http://localhost:7001");
        apiDestinationSinkTask.init(keyValue);
        apiDestinationSinkTask.start(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override
            public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override
            public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override
            public void pause(List<RecordPartition> partitions) {

            }

            @Override
            public void resume(List<RecordPartition> partitions) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("id", "234");
        keyValue.put(HttpConstant.BODYS_CONSTANT, JSONObject.toJSONString(bodyMap));
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.addExtension(HttpConstant.HTTP_QUERY_VALUE, JSONObject.toJSONString(bodyMap));
        connectRecord.setData(JSONObject.toJSONString(new HashMap<>()));
        connectRecordList.add(connectRecord);
        apiDestinationSinkTask.put(connectRecordList);
    }

    @Test
    public void testOAuthAuth() throws InterruptedException {
        ApiDestinationSinkTask apiDestinationSinkTask = new ApiDestinationSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.API_DESTINATION_NAME, "apiDestinationName-15");
        keyValue.put(HttpConstant.ENDPOINT, "http://localhost:7001");
        apiDestinationSinkTask.init(keyValue);
        apiDestinationSinkTask.start(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override
            public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override
            public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override
            public void pause(List<RecordPartition> partitions) {

            }

            @Override
            public void resume(List<RecordPartition> partitions) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("id", "567");
        keyValue.put(HttpConstant.BODYS_CONSTANT, JSONObject.toJSONString(bodyMap));
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.addExtension(HttpConstant.HTTP_QUERY_VALUE, JSONObject.toJSONString(bodyMap));
        connectRecord.setData(JSONObject.toJSONString(new HashMap<>()));
        connectRecordList.add(connectRecord);
        apiDestinationSinkTask.put(connectRecordList);
        Thread.sleep(500000);
    }
}
