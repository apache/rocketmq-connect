package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.client.AbstractHttpClient;
import org.apache.rocketmq.connect.http.sink.client.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.ApiDestionationEntity;
import org.apache.rocketmq.connect.http.sink.entity.ConnectionEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ApiDestinationSinkConnector extends HttpSinkConnector {
    private static final Logger log = LoggerFactory.getLogger(ApiDestinationSinkConnector.class);

    private String apiDestinationName;

    private String regionId;

    private String accessKeyId;

    private String accessKeySecret;

    private String accountId;

    private String endpoint;

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static final AbstractHttpClient HTTP_CLIENT = new ApacheHttpClientImpl();

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        super.taskConfigs(maxTasks);
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put("apiDestinationName", apiDestinationName);
        keyValue.put("regionId", regionId);
        keyValue.put("accessKeyId", accessKeyId);
        keyValue.put("accessKeySecret", accessKeySecret);
        keyValue.put("accountId", accountId);
        keyValue.put("endpoint", endpoint);
        return keyValueList;
    }

    @Override
    public void validate(KeyValue config) {
        super.validate(config);
    }

    @Override
    public void init(KeyValue config) {
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            Map<String, String> map = Maps.newHashMap();
            map.put("Content-Type", "application/json");
            Map<String, String> mapBody = Maps.newHashMap();
            mapBody.put("apiDestinationName", config.getString("apiDestinationName"));
            try {
                final String result = HTTP_CLIENT.execute(config.getString("endpoint") + "/api-destination/getApiDestination", HttpConstant.POST_METHOD, map, mapBody.toString(), "6000");
                final ApiDestionationEntity apiDestionationEntity = JSONObject.parseObject(result, ApiDestionationEntity.class);
                if (HttpConstant.SUCCESS.equalsIgnoreCase(apiDestionationEntity.getCode()) && StringUtils.isNotBlank(apiDestionationEntity.getConnectionName())) {
                    config.put(HttpConstant.URL_PATTERN_CONSTANT, apiDestionationEntity.getHttpApiParameters().getEndpoint());
                    config.put(HttpConstant.METHOD_CONSTANT, apiDestionationEntity.getHttpApiParameters().getMethod());

                    mapBody.put("connectionName", apiDestionationEntity.getConnectionName());
                    final String connectionResult = HTTP_CLIENT.execute(config.getString("endpoint") + "/connection/getConnection", HttpConstant.POST_METHOD, map, mapBody.toString(), "6000");
                    final ConnectionEntity connectionEntity = JSONObject.parseObject(connectionResult, ConnectionEntity.class);
                    if (HttpConstant.SUCCESS.equalsIgnoreCase(connectionEntity.getCode())) {

                    }
                }
            } catch (IOException e) {
                log.error("ApiDestinationSinkConnector | init | error => ", e);
            }
        }, 10, 10, TimeUnit.SECONDS);
        apiDestinationName = config.getString("apiDestinationName");
        regionId = config.getString("regionId");
        accessKeyId = config.getString("accessKeyId");
        accessKeySecret = config.getString("accessKeySecret");
        accountId = config.getString("accountId");
        endpoint = config.getString("endpoint");
        super.init(config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ApiDestinationSinkTask.class;
    }
}
