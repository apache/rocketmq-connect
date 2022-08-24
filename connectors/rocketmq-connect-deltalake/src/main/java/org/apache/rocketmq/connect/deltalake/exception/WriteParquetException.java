package org.apache.rocketmq.connect.deltalake.exception;

/**
 * @author osgoo
 * @date 2022/8/24
 */
public class WriteParquetException extends Exception {
    public WriteParquetException(String msg, Exception e) {
        super(msg, e);
    }
}
