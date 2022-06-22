package org.apache.rocketmq.connect.kafka.connect.adaptor.task;

import io.openmessaging.KeyValue;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TaskConfig;

/**
 * task class setter
 */
public interface TaskClassSetter {
    /**
     * set connector class
     * @param config
     */
    default void setTaskClass(KeyValue config) {
        config.put(TaskConfig.TASK_CLASS_CONFIG, getTaskClass());
    }

    /**
     * get connector class
     */
    String getTaskClass();
}
