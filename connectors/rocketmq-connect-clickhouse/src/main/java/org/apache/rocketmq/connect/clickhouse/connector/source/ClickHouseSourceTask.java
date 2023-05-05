package org.apache.rocketmq.connect.clickhouse.connector.source;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseException;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.connect.clickhouse.connector.config.ClickhouseConfig;

public class ClickHouseSourceTask extends SourceTask {

    private ClickhouseConfig config;

    private ClickHouseNode server;

    @Override public List<ConnectRecord> poll() throws InterruptedException {
        return null;
    }

    int query(ClickHouseNode server, String table) throws ClickHouseException {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
             ClickHouseResponse response = client.read(server)
                 // prefer to use RowBinaryWithNamesAndTypes as it's fully supported
                 // see details at https://github.com/ClickHouse/clickhouse-java/issues/928
                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
                 .query("select * from " + table).execute().get()) {
            int count = 0;
            // or use stream API via response.stream()
            for (ClickHouseRecord r : response.records()) {
                count++;
            }
            return count;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ClickHouseException.forCancellation(e, server);
        } catch (ExecutionException e) {
            throw ClickHouseException.of(e, server);
        }
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
