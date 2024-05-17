package org.apache.rocketmq.connect.oss.service;

import org.apache.rocketmq.connect.oss.exception.LifeCycleException;

public interface LifeCycle {
    void start() throws LifeCycleException;

    boolean isStarted();

    void shutdown();
}
