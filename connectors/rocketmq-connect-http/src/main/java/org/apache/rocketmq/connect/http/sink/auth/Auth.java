package org.apache.rocketmq.connect.http.sink.auth;


import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;

import java.util.Map;

public interface Auth {

    /**
     * Authentication abstract method
     * @param config
     * @return
     */
    Map<String, String> auth(ClientConfig config);
}
