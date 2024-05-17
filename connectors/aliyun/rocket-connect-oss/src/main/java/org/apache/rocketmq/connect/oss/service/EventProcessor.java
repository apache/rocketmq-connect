package org.apache.rocketmq.connect.oss.service;

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.oss.common.cloudEvent.CloudEventSendResult;

public interface EventProcessor extends LifeCycle{
    /**
     * 将CloudEvent事件存入OSS中
     * @return CloudEventSendResult
     */
    CloudEventSendResult send(ConnectRecord connectRecord);


}
