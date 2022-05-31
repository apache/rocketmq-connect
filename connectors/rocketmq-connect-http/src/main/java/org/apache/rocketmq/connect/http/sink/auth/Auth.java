package org.apache.rocketmq.connect.http.sink.auth;

import org.apache.rocketmq.connect.http.sink.common.ClientConfig;

import java.util.Map;

public interface Auth {

    Map<String, String> auth(ClientConfig config);

}
