package org.apache.rocketmq.connect.clickhouse.connector.source;

import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.util.List;
import org.apache.rocketmq.connect.clickhouse.connector.config.ClickhouseConfig;

public class ClickHouseSourceTask extends SourceTask {

    private ClickhouseConfig config;

    private ClickHouseNode server;

    @Override public List<ConnectRecord> poll() throws InterruptedException {
        return null;
    }

    @Override public void start(KeyValue keyValue) {
        this.config.load(keyValue);

        this.server = ClickHouseNode.builder()
            .host(config.getClickHouseHost())
            .port(ClickHouseProtocol.HTTP, Integer.valueOf(config.getClickHousePort()))
            .database(config.getDatabase()).credentials(getCredentials(config))
            .build();

    }

    private ClickHouseCredentials getCredentials (ClickhouseConfig config){
        if (config.getUserName() != null && config.getPassWord() != null) {
            return ClickHouseCredentials.fromUserAndPassword(config.getUserName(), config.getPassWord());
        }
        if (config.getAccessToken() != null) {
            return ClickHouseCredentials.fromAccessToken(config.getAccessToken());
        }
        throw new RuntimeException("Credentials cannot be empty!");

    }

    @Override public void stop() {
        this.server = null;
    }
}
