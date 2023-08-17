package org.apache.rocket.connect.metrics.export.sink.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocket.connect.metrics.export.sink.util.ServiceProvicerUtil;

public class MetricsExportSinkConnector extends SinkConnector {
    private KeyValue config;

    private List<MetricsExporter> metricsExporters;
    {
        metricsExporters = ServiceProvicerUtil.getMetricsExporterServices();
    }

    @Override public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        configs.add(config);
        return configs;
    }

    @Override public Class<? extends Task> taskClass() {
        return MetricsExportSinkTask.class;
    }

    @Override public void start(KeyValue config) {
        this.config = config;
    }

    @Override public void stop() {
        this.config = null;
    }

    @Override public void validate(KeyValue config) {
        for (MetricsExporter exporter: metricsExporters) {
            exporter.validate(config);
        }
    }
}
