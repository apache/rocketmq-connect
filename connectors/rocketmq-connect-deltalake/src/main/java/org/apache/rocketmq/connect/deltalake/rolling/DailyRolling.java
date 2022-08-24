package org.apache.rocketmq.connect.deltalake.rolling;

import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public class DailyRolling implements StoreFileRolling {
    private DeltalakeConnectConfig deltalakeConnectConfig;
    // todo support partition columns
    private String additionalPartitionColumns;
    private final static String FILE_SPLITER = "/";
    private final static String FILE_CONCATOR = "#";
    public DailyRolling(DeltalakeConnectConfig deltalakeConnectConfig) {
        this.deltalakeConnectConfig = deltalakeConnectConfig;
        this.additionalPartitionColumns = deltalakeConnectConfig.getAdditionalPartitionColumns();
    }

    @Override
    public String generateStoreDir(RecordPosition partition, long timestamp) {
        RecordPartition recordPartition = partition.getPartition();
        String topic = (String) recordPartition.getPartition().get("topic");
        String queue = (String) recordPartition.getPartition().get("queue");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date now = new Date(timestamp);
        String currentStr = simpleDateFormat.format(now);
        return FILE_SPLITER + topic + FILE_SPLITER + queue + FILE_SPLITER + currentStr + FILE_SPLITER;
    }

    @Override
    public String generateStoreFileName(RecordPosition partition, long timestamp) {
        RecordPartition recordPartition = partition.getPartition();
        String topic = (String) recordPartition.getPartition().get("topic");
        String queue = (String) recordPartition.getPartition().get("queue");
        long offset = (Long) recordPartition.getPartition().get("offset") % (deltalakeConnectConfig.getParquetSegmentLength());
        String offsetStr = String.format("%20d", offset);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
        Date now = new Date(timestamp);
        String currentStr = simpleDateFormat.format(now);
        return FILE_SPLITER + topic + FILE_CONCATOR + queue + FILE_CONCATOR + offsetStr + FILE_CONCATOR + currentStr + FILE_CONCATOR + deltalakeConnectConfig.getCompressType() + ".parquet";
    }
}
