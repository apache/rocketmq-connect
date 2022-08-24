package org.apache.rocketmq.connect.deltalake.rolling;

import io.openmessaging.connector.api.data.RecordPosition;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public interface StoreFileRolling {
    /**
     * generate store dir by RecordPosition and timestamp
     * @param partition
     * @param timestamp
     * @return
     */
    String generateStoreDir(RecordPosition partition, long timestamp);

    /**
     * generate store file name by RecordPosition
     * @param partition
     * @param timestamp
     * @return
     */
    String generateStoreFileName(RecordPosition partition, long timestamp);

}
