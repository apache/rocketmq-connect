package org.apache.rocketmq.connect.http.sink.auth;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ApiKeyImpl implements Auth {
    private static final Logger log = LoggerFactory.getLogger(ApiKeyImpl.class);

    @Override
    public Map<String, String> auth(ClientConfig config) {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            if (StringUtils.isNotBlank(config.getApiKeyName()) && StringUtils.isNotBlank(config.getApiKeyValue())) {
                headMap.put(config.getApiKeyName(), config.getApiKeyValue());
            }
        } catch (Exception e) {
            log.error("ApiKeyImpl | auth | error => ", e);
        }
        return headMap;
    }
}
