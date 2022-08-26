package org.apache.rocketmq.connect.runtime.config;

import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;

/**
 * source connector config
 */
public class SourceConnectorConfig extends ConnectorConfig {

    public static final String CONNECT_TOPICNAME = "connect.topicname";

    public SourceConnectorConfig(ConnectKeyValue config) {
        super(config);
    }

}
