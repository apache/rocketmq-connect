package org.apache.rocketmq.connect.deltalake.rolling;

import io.openmessaging.connector.api.data.RecordPosition;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public class DailyPartition implements Partition {
    private String additionalPartitionColumns;
    public DailyPartition() {

    }

    public DailyPartition(String additionalPartitionColumns) {
        this.additionalPartitionColumns = additionalPartitionColumns;
    }

    @Override
    public String storeDir(RecordPosition partition, long timestamp) {
        return null;
    }

    @Override
    public String storeFileName(RecordPosition partition) {
        return null;
    }
}
