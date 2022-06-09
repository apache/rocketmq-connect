package org.apache.rocketmq.connect.http.sink.auth;

import com.google.common.collect.Maps;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.common.ClientConfig;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BasicAuthImpl implements Auth {
    private static final Logger log = LoggerFactory.getLogger(BasicAuthImpl.class);


    @Override
    public Map<String, String> auth(ClientConfig config) {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            if (StringUtils.isNotBlank(config.getBasicUser()) && StringUtils.isNotBlank(config.getBasicPassword())) {
                String authorizationValue = config.getBasicUser() + ":" + config.getBasicPassword();
                headMap.put(HttpConstant.AUTHORIZATION, "Basic " + Base64.encode(authorizationValue.getBytes(StandardCharsets.UTF_8)));
            }
        } catch (Exception e) {
            log.error("BasicAuthImpl | auth | error => ", e);
        }
        return headMap;
    }
}
