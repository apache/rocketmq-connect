package org.apache.rocketmq.connect.http;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.connect.http.auth.AbstractHttpClient;
import org.apache.rocketmq.connect.http.auth.HttpCallback;
import org.apache.rocketmq.connect.http.constant.AuthTypeEnum;
import org.apache.rocketmq.connect.http.constant.HttpConstant;
import org.apache.rocketmq.connect.http.entity.HttpRequest;
import org.apache.rocketmq.connect.http.entity.TokenEntity;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkTaskTest {

    @Mock
    AbstractHttpClient httpClient;

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testHttpPutWithoutHeaderAndQueryAndAuth() throws Exception {
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            Assert.assertEquals("http://localhost:8080/api", httpRequest.getUrl());
            Assert.assertEquals("POST", httpRequest.getMethod());
            Assert.assertEquals("{token=token}", httpRequest.getHeaderMap().toString());
            Assert.assertEquals("123", httpRequest.getBody());
            HttpCallback httpCallback = invocationOnMock.getArgument(1);
            httpCallback.getCountDownLatch()
                    .countDown();
            return null;
        }).when(httpClient).executeNotReturn(any(), any());
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL, "http://localhost:8080/api");
        keyValue.put(HttpConstant.METHOD, "POST");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("form", "CONSTANT");
        bodyMap.put("value", "123");
        keyValue.put(HttpConstant.BODY, "123");
        keyValue.put(HttpConstant.TOKEN, "token");
        httpSinkTask.validate(keyValue);

        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData(JSONObject.toJSONString(new HashMap<>()));
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        Field field = httpSinkTask.getClass().getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(httpSinkTask, httpClient);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }

    @Test
    public void testHttpPutWithHeaderAndQueryWithoutAuth() throws Exception {
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            System.out.println(new Gson().toJson(httpRequest));
            Assert.assertEquals("http://localhost:8080/api?action=action&accountId=accountId",
                    httpRequest.getUrl());
            Assert.assertEquals("POST", httpRequest.getMethod());
            Assert.assertEquals("{meetingName=swqd, id=45ef4dewdwe1-7c35-447a-bd93-fab****, userId=199525, groupId=456, token=token}", httpRequest.getHeaderMap().toString());
            Assert.assertEquals("{\"form\":\"CONSTANT\",\"value\":\"123\"}", httpRequest.getBody());
            HttpCallback httpCallback = invocationOnMock.getArgument(1);
            httpCallback.getCountDownLatch()
                    .countDown();
            return null;
        }).when(httpClient).executeNotReturn(any(), any());
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL, "http://localhost:8080/api");
        keyValue.put(HttpConstant.METHOD, "POST");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        keyValue.put(HttpConstant.SOCKS5_ENDPOINT, "127.0.0.1");
        keyValue.put(HttpConstant.SOCKS5_USERNAME, "xxxx");
        keyValue.put(HttpConstant.SOCKS5_PASSWORD, "xxxx");
        keyValue.put(HttpConstant.HEADER_PARAMETERS, "{\n" +
                "  \"meetingName\": \"swqd\",\n" +
                "  \"groupId\": \"456\"\n" +
                "}");
        keyValue.put(HttpConstant.FIXED_HEADER_PARAMETERS, "{\n" +
                "  \"id\": \"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "  \"userId\": \"199525\"\n" +
                "}");
        keyValue.put(HttpConstant.QUERY_PARAMETERS, "{\n" +
                "  \"action\": \"action\"" +
                "}");
        keyValue.put(HttpConstant.FIXED_QUERY_PARAMETERS, "{\n" +
                "  \"accountId\": \"accountId\"" +
                "}");
        keyValue.put(HttpConstant.TOKEN, "token");
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("form", "CONSTANT");
        bodyMap.put("value", "123");
        keyValue.put(HttpConstant.BODY, JSONObject.toJSONString(bodyMap));
        httpSinkTask.validate(keyValue);

        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        String cloudEvent = "{\n" +
                "    \"data\":{\n" +
                "        \"meetingName\":\"swqd\",\n" +
                "        \"groupId\":\"456\",\n" +
                "        \"action\":\"camera_off\",\n" +
                "        \"time\":1590592527490,\n" +
                "        \"userId\":\"199525\",\n" +
                "        \"meetingUUID\":\"hz-20864c8f-b10d-45cd-9935-884bca1b****\"\n" +
                "    },\n" +
                "    \"id\":\"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "    \"source\":\"acs:aliyuncvc\",\n" +
                "    \"specversion\":\"1.0\",\n" +
                "    \"subject\":\"acs.aliyuncvc:cn-hangzhou:{AccountId}:215672\",\n" +
                "    \"time\":\"2020-11-19T21:04:41+08:00\",\n" +
                "    \"type\":\"aliyuncvc:MeetingEvent:MemberOperate\",\n" +
                "    \"aliyunaccountid\":\"123456789098****\",\n" +
                "    \"aliyunpublishtime\":\"2020-11-19T21:04:42.179PRC\",\n" +
                "    \"aliyuneventbusname\":\"default\",\n" +
                "    \"aliyunregionid\":\"cn-hangzhou\",\n" +
                "    \"aliyunpublishaddr\":\"172.25.XX.XX\"\n" +
                "}";
        connectRecord.setData(cloudEvent);
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        Field field = httpSinkTask.getClass().getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(httpSinkTask, httpClient);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }

    @Test
    public void testHttpPutWithHeaderAndQueryWithBasicAuth() throws Exception {
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            System.out.println(new Gson().toJson(httpRequest));
            Assert.assertEquals("http://localhost:8080/api?action=action&accountId=accountId",
                    httpRequest.getUrl());
            Assert.assertEquals("POST", httpRequest.getMethod());
            Assert.assertEquals("{Authorization=Basic dXNlcm5hbWU6cGFzc3dvcmQ=, meetingName=swqd, id=45ef4dewdwe1-7c35-447a-bd93-fab****, userId=199525, groupId=456, token=token}", httpRequest.getHeaderMap().toString());
            Assert.assertEquals("{\"form\":\"CONSTANT\",\"value\":\"123\"}", httpRequest.getBody());
            HttpCallback httpCallback = invocationOnMock.getArgument(1);
            httpCallback.getCountDownLatch()
                    .countDown();
            return null;
        }).when(httpClient).executeNotReturn(any(), any());
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL, "http://localhost:8080/api");
        keyValue.put(HttpConstant.METHOD, "POST");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        keyValue.put(HttpConstant.SOCKS5_ENDPOINT, "127.0.0.1");
        keyValue.put(HttpConstant.SOCKS5_USERNAME, "xxxx");
        keyValue.put(HttpConstant.SOCKS5_PASSWORD, "xxxx");
        keyValue.put(HttpConstant.HEADER_PARAMETERS, "{\n" +
                "  \"meetingName\": \"swqd\",\n" +
                "  \"groupId\": \"456\"\n" +
                "}");
        keyValue.put(HttpConstant.FIXED_HEADER_PARAMETERS, "{\n" +
                "  \"id\": \"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "  \"userId\": \"199525\"\n" +
                "}");
        keyValue.put(HttpConstant.QUERY_PARAMETERS, "{\n" +
                "  \"action\": \"action\"" +
                "}");
        keyValue.put(HttpConstant.FIXED_QUERY_PARAMETERS, "{\n" +
                "  \"accountId\": \"accountId\"" +
                "}");
        keyValue.put(HttpConstant.TOKEN, "token");
        keyValue.put(HttpConstant.AUTH_TYPE, "BASIC_AUTH");
        keyValue.put(HttpConstant.BASIC_USERNAME, "username");
        keyValue.put(HttpConstant.BASIC_PASSWORD, "password");
        Map<String, String> bodyMap = Maps.newHashMap();
        bodyMap.put("form", "CONSTANT");
        bodyMap.put("value", "123");
        keyValue.put(HttpConstant.BODY, JSONObject.toJSONString(bodyMap));
        httpSinkTask.validate(keyValue);

        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        String cloudEvent = "{\n" +
                "    \"data\":{\n" +
                "        \"meetingName\":\"swqd\",\n" +
                "        \"groupId\":\"456\",\n" +
                "        \"action\":\"camera_off\",\n" +
                "        \"time\":1590592527490,\n" +
                "        \"userId\":\"199525\",\n" +
                "        \"meetingUUID\":\"hz-20864c8f-b10d-45cd-9935-884bca1b****\"\n" +
                "    },\n" +
                "    \"id\":\"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "    \"source\":\"acs:aliyuncvc\",\n" +
                "    \"specversion\":\"1.0\",\n" +
                "    \"subject\":\"acs.aliyuncvc:cn-hangzhou:{AccountId}:215672\",\n" +
                "    \"time\":\"2020-11-19T21:04:41+08:00\",\n" +
                "    \"type\":\"aliyuncvc:MeetingEvent:MemberOperate\",\n" +
                "    \"aliyunaccountid\":\"123456789098****\",\n" +
                "    \"aliyunpublishtime\":\"2020-11-19T21:04:42.179PRC\",\n" +
                "    \"aliyuneventbusname\":\"default\",\n" +
                "    \"aliyunregionid\":\"cn-hangzhou\",\n" +
                "    \"aliyunpublishaddr\":\"172.25.XX.XX\"\n" +
                "}";
        connectRecord.setData(cloudEvent);
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        Field field = httpSinkTask.getClass().getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(httpSinkTask, httpClient);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }


    @Test
    public void testHttpPutWithHeaderAndQueryWithApiKey() throws Exception {
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            System.out.println(new Gson().toJson(httpRequest));
            Assert.assertEquals("http://localhost:8080/api?action=action&accountId=accountId",
                    httpRequest.getUrl());
            Assert.assertEquals("POST", httpRequest.getMethod());
            Assert.assertEquals("{meetingName=swqd, id=45ef4dewdwe1-7c35-447a-bd93-fab****, userId=199525, groupId=456, token=token, username=password}", httpRequest.getHeaderMap().toString());
            Assert.assertEquals("\"data\"", httpRequest.getBody());
            HttpCallback httpCallback = invocationOnMock.getArgument(1);
            httpCallback.getCountDownLatch()
                    .countDown();
            return null;
        }).when(httpClient).executeNotReturn(any(), any());
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL, "http://localhost:8080/api");
        keyValue.put(HttpConstant.METHOD, "POST");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        keyValue.put(HttpConstant.SOCKS5_ENDPOINT, "127.0.0.1");
        keyValue.put(HttpConstant.SOCKS5_USERNAME, "xxxx");
        keyValue.put(HttpConstant.SOCKS5_PASSWORD, "xxxx");
        keyValue.put(HttpConstant.HEADER_PARAMETERS, "{\n" +
                "  \"meetingName\": \"swqd\",\n" +
                "  \"groupId\": \"456\"\n" +
                "}");
        keyValue.put(HttpConstant.FIXED_HEADER_PARAMETERS, "{\n" +
                "  \"id\": \"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "  \"userId\": \"199525\"\n" +
                "}");
        keyValue.put(HttpConstant.QUERY_PARAMETERS, "{\n" +
                "  \"action\": \"action\"" +
                "}");
        keyValue.put(HttpConstant.FIXED_QUERY_PARAMETERS, "{\n" +
                "  \"accountId\": \"accountId\"" +
                "}");
        keyValue.put(HttpConstant.TOKEN, "token");
        keyValue.put(HttpConstant.AUTH_TYPE, AuthTypeEnum.API_KEY.getAuthType());
        keyValue.put(HttpConstant.API_KEY_USERNAME, "username");
        keyValue.put(HttpConstant.API_KEY_PASSWORD, "password");
        httpSinkTask.validate(keyValue);
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("data");
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        Field field = httpSinkTask.getClass().getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(httpSinkTask, httpClient);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }

    @Test
    public void testHttpPutWithHeaderAndQueryWithOAuth() throws Exception {
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            System.out.println(new Gson().toJson(httpRequest));
            Assert.assertEquals("http://localhost:8080/api?action=action&accountId=accountId",
                    httpRequest.getUrl());
            Assert.assertEquals("POST", httpRequest.getMethod());
            Assert.assertEquals("{Authorization=Bearer oAuthAccessToken, meetingName=swqd, id=45ef4dewdwe1-7c35-447a-bd93-fab****, userId=199525, groupId=456, token=token}", httpRequest.getHeaderMap().toString());
            Assert.assertEquals("\"data\"", httpRequest.getBody());
            HttpCallback httpCallback = invocationOnMock.getArgument(1);
            httpCallback.getCountDownLatch()
                    .countDown();
            return null;
        }).when(httpClient).executeNotReturn(any(), any());
        // for OAuth request
        doAnswer(invocationOnMock -> {
            HttpRequest httpRequest = invocationOnMock.getArgument(0);
            Assert.assertEquals("{\"url\":\"oAuthEndpoint?headerKey1\\u003dheaderValue1\\u0026client_secret\\u003doAuthClientSecret\\u0026client_id\\u003doAuthClientId\",\"method\":\"POST\",\"headerMap\":{\"headerKey1\":\"headerValue1\"},\"body\":\"oAuthBody\",\"timeout\":\"3000\"}", new Gson().toJson(httpRequest));
            TokenEntity tokenEntity = new TokenEntity();
            tokenEntity.setAccessToken("oAuthAccessToken");
            tokenEntity.setTokenType("tokenType");
            tokenEntity.setExpiresIn(100);
            tokenEntity.setError("error");
            tokenEntity.setMessage("message");
            tokenEntity.setPath("path");
            tokenEntity.setScope("scope");
            tokenEntity.setTokenTimestamp("tokenTimestamp");
            tokenEntity.setTimestamp("timestamp");
            tokenEntity.setExampleParameter("exampleParameter");
            tokenEntity.setStatus("status");
            return JSONObject.toJSONString(tokenEntity);
        }).when(httpClient).execute(any(), any());
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL, "http://localhost:8080/api");
        keyValue.put(HttpConstant.METHOD, "POST");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        keyValue.put(HttpConstant.SOCKS5_ENDPOINT, "127.0.0.1");
        keyValue.put(HttpConstant.SOCKS5_USERNAME, "xxxx");
        keyValue.put(HttpConstant.SOCKS5_PASSWORD, "xxxx");
        keyValue.put(HttpConstant.HEADER_PARAMETERS, "{\n" +
                "  \"meetingName\": \"swqd\",\n" +
                "  \"groupId\": \"456\"\n" +
                "}");
        keyValue.put(HttpConstant.FIXED_HEADER_PARAMETERS, "{\n" +
                "  \"id\": \"45ef4dewdwe1-7c35-447a-bd93-fab****\",\n" +
                "  \"userId\": \"199525\"\n" +
                "}");
        keyValue.put(HttpConstant.QUERY_PARAMETERS, "{\n" +
                "  \"action\": \"action\"" +
                "}");
        keyValue.put(HttpConstant.FIXED_QUERY_PARAMETERS, "{\n" +
                "  \"accountId\": \"accountId\"" +
                "}");
        keyValue.put(HttpConstant.TOKEN, "token");
        keyValue.put(HttpConstant.AUTH_TYPE, "OAUTH_AUTH");
        String oAuthEndpoint = "oAuthEndpoint";
        String oAuthHttpMethod = "POST";
        String oAuthClientId = "oAuthClientId";
        String oAuthClientSecret = "oAuthClientSecret";
        String oAuthHeaderParameters = "{\n" +
                "  \"headerKey1\": \"headerValue1\"" +
                "}";
        String oAuthQueryParameters = "{\n" +
                "  \"queryKey1\": \"queryValue1\"" +
                "}";
        String oAuthBody = "oAuthBody";
        keyValue.put(HttpConstant.OAUTH_ENDPOINT, oAuthEndpoint);
        keyValue.put(HttpConstant.OAUTH_HTTP_METHOD, oAuthHttpMethod);
        keyValue.put(HttpConstant.OAUTH_CLIENT_ID, oAuthClientId);
        keyValue.put(HttpConstant.OAUTH_CLIENT_SECRET, oAuthClientSecret);
        keyValue.put(HttpConstant.OAUTH_HEADER_PARAMETERS, oAuthHeaderParameters);
        keyValue.put(HttpConstant.OAUTH_QUERY_PARAMETERS, oAuthQueryParameters);
        keyValue.put(HttpConstant.OAUTH_BODY, oAuthBody);
        httpSinkTask.validate(keyValue);
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("data");
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        Field field = httpSinkTask.getClass().getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(httpSinkTask, httpClient);
        httpSinkTask.updateAuth(AuthTypeEnum.OAUTH_CLIENT_CREDENTIALS.getAuthType(), null, null, null, null, oAuthEndpoint, oAuthHttpMethod, oAuthClientId, oAuthClientSecret, oAuthHeaderParameters, oAuthHeaderParameters, oAuthBody);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }

    @Test
    @Ignore
    // use for invoke local service
    public void test() throws Exception {
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        // test basic
        keyValue.put(HttpConstant.URL, "http://localhost:8080");
        keyValue.put(HttpConstant.METHOD, "GET");
        keyValue.put(HttpConstant.TIMEOUT, 60000);
        keyValue.put(HttpConstant.FIXED_HEADER_PARAMETERS, "{\n" +
                "  \"Content-Type\": \"application/json\",\n" +
                "  \"user\": \"user\"" +
                "}");
        httpSinkTask.validate(keyValue);
        List<ConnectRecord> connectRecordList = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("data");
        connectRecordList.add(connectRecord);
        httpSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        httpSinkTask.start(keyValue);
        httpSinkTask.put(connectRecordList);
        httpSinkTask.stop();
    }
}
