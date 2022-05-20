package org.apache.rocketmq.connect.runtime.controller;

/**
 * connect controller
 */
public interface ConnectController {

    /**
     * start controller
     */
    void start();

    /**
     * shutdown controller
     */
    void shutdown();

}
