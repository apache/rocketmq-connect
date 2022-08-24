package org.apache.rocketmq.connect.deltalake.config;

import org.apache.avro.Schema;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeConnectConfig {
    private Schema schema;

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
