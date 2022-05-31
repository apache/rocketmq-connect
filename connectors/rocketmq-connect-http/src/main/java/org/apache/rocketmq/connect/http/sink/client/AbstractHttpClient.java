package org.apache.rocketmq.connect.http.sink.client;


import org.apache.rocketmq.connect.http.sink.common.ClientConfig;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;

import java.io.IOException;
import java.util.Map;

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
    String execute(HttpRequest httpRequest) throws IOException;

    void close();

}
