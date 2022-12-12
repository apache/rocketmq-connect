package org.apache.rocketmq.connect.http.sink;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.http.sink.constant.AuthTypeEnum;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;

public class HttpSinkTaskTest {

    private final HttpSinkTask httpSinkTask = new HttpSinkTask();

    @Test(expected = RuntimeException.class)
    public void testPutBasic() {
        KeyValue keyValue = new DefaultKeyValue();
        String apiDestinationName = UUID.randomUUID().toString();
        keyValue.put(HttpConstant.URL_PATTERN_CONSTANT, "http://127.0.0.1:7001/xxxx?id=" + apiDestinationName);
        keyValue.put(HttpConstant.METHOD_CONSTANT, "POST");
        keyValue.put(HttpConstant.BASIC_USER_CONSTANT, "xxxx");
        keyValue.put(HttpConstant.BASIC_PASSWORD_CONSTANT, "xxxx");
        keyValue.put(HttpConstant.AUTH_TYPE_CONSTANT, AuthTypeEnum.BASIC.getAuthType());
        httpSinkTask.setScheduledExecutorService(Executors.newSingleThreadScheduledExecutor());
        httpSinkTask.start(keyValue);
        List<ConnectRecord> sinkRecords = new ArrayList<>(11);
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        sinkRecords.add(connectRecord);
        httpSinkTask.put(sinkRecords);
    }

    @Test(expected = RuntimeException.class)
    public void testPutApiKey() {
        KeyValue keyValue = new DefaultKeyValue();
        String apiDestinationName = UUID.randomUUID().toString();
        keyValue.put(HttpConstant.URL_PATTERN_CONSTANT, "http://127.0.0.1:7001/xxxx?id=" + apiDestinationName);
        keyValue.put(HttpConstant.METHOD_CONSTANT, "POST");
        keyValue.put(HttpConstant.API_KEY_NAME, "Token");
        keyValue.put(HttpConstant.API_KEY_VALUE, "xxxx");
        keyValue.put(HttpConstant.AUTH_TYPE_CONSTANT, AuthTypeEnum.API_KEY.getAuthType());
        httpSinkTask.setScheduledExecutorService(Executors.newSingleThreadScheduledExecutor());
        httpSinkTask.start(keyValue);
        List<ConnectRecord> sinkRecords = new ArrayList<>(11);
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        sinkRecords.add(connectRecord);
        httpSinkTask.put(sinkRecords);
    }

    @Test(expected = RuntimeException.class)
    public void testPutOAuth2() {
        KeyValue keyValue = new DefaultKeyValue();
        String apiDestinationName = UUID.randomUUID().toString();
        keyValue.put(HttpConstant.URL_PATTERN_CONSTANT, "http://127.0.0.1:7001/xxxx?id=" + apiDestinationName);
        keyValue.put(HttpConstant.METHOD_CONSTANT, "POST");
        keyValue.put(HttpConstant.OAUTH2_CLIENTID_CONSTANT, "clientId");
        keyValue.put(HttpConstant.OAUTH2_CLIENTSECRET_CONSTANT, "clientSecret");
        keyValue.put(HttpConstant.OAUTH2_HTTP_METHOD_CONSTANT, "POST");
        keyValue.put(HttpConstant.OAUTH2_ENDPOINT_CONSTANT, "http://127.0.0.1:7001/oauth/token");
        keyValue.put(HttpConstant.AUTH_TYPE_CONSTANT, AuthTypeEnum.OAUTH_CLIENT_CREDENTIALS.getAuthType());
        Map<String, String> queryStringParameters = Maps.newHashMap();
        queryStringParameters.put("grant_type", "xxxx");
        queryStringParameters.put("scope", "xxxx");
        keyValue.put(HttpConstant.QUERY_STRING_PARAMETERS_CONSTANT, new Gson().toJson(queryStringParameters));
        Map<String, String> headerParameters = Maps.newHashMap();
        headerParameters.put("Content-Type", "xxxx");
        headerParameters.put("Authorization", "xxxx");
        keyValue.put(HttpConstant.HEADER_PARAMETERS_CONSTANT, new Gson().toJson(headerParameters));
        httpSinkTask.setScheduledExecutorService(Executors.newSingleThreadScheduledExecutor());
        httpSinkTask.start(keyValue);
        List<ConnectRecord> sinkRecords = new ArrayList<>(11);
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        sinkRecords.add(connectRecord);
        httpSinkTask.put(sinkRecords);
    }
}
