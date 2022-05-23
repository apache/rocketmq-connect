package org.apache.kafka.connect.transforms;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;

/**
 * base transformation
 * @param <R>
 */
public abstract class BaseTransformation<R extends ConnectRecord> implements Transform<R> {

    /**
     * validate config
     * @param config
     */
    @Override
    public void validate(KeyValue config){

    }

    /**
     * init config
     * @param config
     */
    @Override
    public void init(KeyValue config) {
        this.validate(config);
        this.configure(config);
    }

    /**
     * set config
     * @param config
     */
    public abstract void configure(KeyValue config);
}
