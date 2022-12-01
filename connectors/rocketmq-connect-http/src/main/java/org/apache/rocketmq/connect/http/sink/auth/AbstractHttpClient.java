package org.apache.rocketmq.connect.http.sink.auth;


import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;

import java.io.IOException;

public interface AbstractHttpClient {

    /**
     *
     * @param config
     */
    void init(ClientConfig config);

    /**
     *
     * @return
     * @throws IOException
     */
    String execute(HttpRequest httpRequest, HttpCallback httpCallback) throws Exception;

    void close();

}
