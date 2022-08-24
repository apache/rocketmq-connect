package org.apache.rocketmq.connect.deltalake.rolling;

import io.openmessaging.connector.api.data.RecordPosition;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public interface Partition {
    /**
     * generate store dir by RecordPosition and timestamp
     * @param partition
     * @param timestamp
     * @return
     */
    String storeDir(RecordPosition partition, long timestamp);

    /**
     * generate store file name by RecordPosition
     * @param partition
     * @return
     */
    String storeFileName(RecordPosition partition);

}
