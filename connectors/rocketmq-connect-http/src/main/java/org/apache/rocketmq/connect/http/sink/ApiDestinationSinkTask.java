package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;

import java.util.List;

public class ApiDestinationSinkTask extends HttpSinkTask{

    @Override
    public void init(KeyValue config) {
        super.init(config);
    }

    @Override
    public void validate(KeyValue config) {
        super.validate(config);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        super.put(sinkRecords);
    }
}
