package org.apache.rocket.connect.metrics.export.sink.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.util.List;

public interface MetricsExporter {
    void export(List<ConnectRecord> sinkRecords);

    void validate(KeyValue config);

    void start(KeyValue config);

    void stop();

}
