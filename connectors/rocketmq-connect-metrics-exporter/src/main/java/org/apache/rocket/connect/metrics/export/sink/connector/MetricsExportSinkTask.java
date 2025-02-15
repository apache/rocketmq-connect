package org.apache.rocket.connect.metrics.export.sink.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.List;
import org.apache.rocket.connect.metrics.export.sink.util.ServiceProvicerUtil;


public class MetricsExportSinkTask extends SinkTask {
    private List<MetricsExporter> metricsExporters;

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        for (MetricsExporter exporter : metricsExporters) {
            exporter.export(sinkRecords);
        }
    }

    @Override public void start(KeyValue config) {
        for (MetricsExporter exporter : metricsExporters) {
            exporter.start(config);
        }
    }

    @Override public void stop() {
        for (MetricsExporter exporter : metricsExporters) {
            exporter.stop();
        }
    }

    @Override public void init(SinkTaskContext sinkTaskContext) {
        super.init(sinkTaskContext);
        metricsExporters = ServiceProvicerUtil.getMetricsExporterServices();
    }
}
