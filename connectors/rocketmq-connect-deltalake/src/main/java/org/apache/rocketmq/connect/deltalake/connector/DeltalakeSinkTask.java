package org.apache.rocketmq.connect.deltalake.connector;

import org.apache.rocketmq.connect.deltalake.config.ConfigUtil;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;
import org.apache.rocketmq.connect.deltalake.writer.DeltalakeWriterOnHdfs;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DeltalakeSinkTask.class);
    private DeltalakeConnectConfig deltalakeConnectConfig;
    private DeltalakeWriterOnHdfs writer;

    @Override
    public void put(List<ConnectRecord> list) throws ConnectException {
        writer.writeEntries(list);
    }

    @Override
    public void start(KeyValue props) {
        try {
            ConfigUtil.load(props, this.deltalakeConnectConfig);
            log.info("init data source success");
        } catch (Exception e) {
            log.error("Cannot start Hudi Sink Task because of configuration error{}", e);
        }
        try {
            writer = new DeltalakeWriterOnHdfs(deltalakeConnectConfig);
        } catch (Throwable e) {
            log.error("fail to start updater{}", e);
        }
    }

    @Override
    public void stop() {

    }



}
