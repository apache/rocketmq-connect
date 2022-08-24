package org.apache.rocketmq.connect.deltalake.writer;

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.deltalake.exception.WriteParquetException;

import java.util.Collection;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public interface DeltalakeWriter {
    void writeEntries(Collection<ConnectRecord> entries) throws WriteParquetException;
}
