package org.apache.rocketmq.connect.runtime.connectorwrapper;

public enum WorkerTaskState {
    NEW,
    PENDING,
    RUNNING,
    ERROR,
    STOPPING,
    STOPPED,
    TERMINATED,
}
