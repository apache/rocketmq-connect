package org.apache.rocketmq.connect.deltalake.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;

import java.util.List;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeSinkConnector extends SinkConnector {

    @Override
    public void start(KeyValue keyValue) {

    }

    @Override
    public void stop() {

    }

    @Override
    public List<KeyValue> taskConfigs(int i) {
        return null;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

}
