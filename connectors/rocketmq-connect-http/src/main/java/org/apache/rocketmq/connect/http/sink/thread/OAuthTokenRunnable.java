package org.apache.rocketmq.connect.http.sink.thread;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.auth.OAuthClientImpl;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.entity.TokenEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

public class OAuthTokenRunnable implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(OAuthTokenRunnable.class);

    @Override
    public void run() {
        log.info("OAuthTokenRunnable | run");
        OAuthClientImpl.OAUTH_MAP.forEach((oAuthEntity1, tokenEntity) -> {
            String resultToken = "";
            long tokenTimestamp = Long.parseLong(tokenEntity.getTokenTimestamp()) + (tokenEntity.getExpiresIn() * 1000);
            log.info("OAuthTokenRunnable | run | tokenTimestamp : {} | system.currentTimeMillis : {} | boolean : {}", tokenTimestamp, System.currentTimeMillis(), System.currentTimeMillis() > tokenTimestamp);
            if (System.currentTimeMillis() > tokenTimestamp) {
                log.info("OAuthTokenRunnable | run | update token");
                HttpRequest httpRequest = new HttpRequest();
                try {
                    resultToken = OAuthClientImpl.getResultToken(oAuthEntity1, new HashMap<>(16), resultToken, httpRequest);
                } catch (IOException e) {
                    log.error("BasicAuthImpl | auth | scheduledExecutorService | error => ", e);
                }
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(token.getAccessToken())) {
                        OAuthClientImpl.OAUTH_MAP.put(oAuthEntity1, token);
                    } else {
                        throw new RuntimeException(token.getError());
                    }
                }
            }
        });
    }
}
