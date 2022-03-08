package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.connect.http.sink.common.OkHttpUtils;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

    private String url;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(connectRecord -> {
                OkHttpUtils.builder()
                        .url(url)
                        .addParam(HttpConstant.DATA_CONSTANT, JSON.toJSONString(connectRecord.getData()))
                        .post(true)
                        .sync();
            });
        } catch (Exception e) {
            log.error("HttpSinkTask | put | error => ", e);
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString("url"))) {
            throw new RuntimeException("http required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
    }

    @Override
    public void stop() {

    }
}
