package org.apache.rocketmq.connect.http.sink.auth;

import org.apache.rocketmq.connect.http.sink.common.ClientConfig;
import org.apache.rocketmq.connect.http.sink.enums.AuthTypeEnum;

import java.util.HashMap;
import java.util.Map;

public class AuthFactory {

    private static final Map<String, Auth> AUTH_MAP = new HashMap<>();

    static {
        AUTH_MAP.put(AuthTypeEnum.BASIC.getAuthType(), new BasicAuthImpl());
        AUTH_MAP.put(AuthTypeEnum.OAUTH_CLIENT_CREDENTIALS.getAuthType(), new OAuthClientImpl());
        AUTH_MAP.put(AuthTypeEnum.API_KEY.getAuthType(), new ApiKeyImpl());
    }

    public static Map<String, String> auth(ClientConfig config) {
        return AUTH_MAP.get(config.getAuthType()).auth(config);
    }

}
