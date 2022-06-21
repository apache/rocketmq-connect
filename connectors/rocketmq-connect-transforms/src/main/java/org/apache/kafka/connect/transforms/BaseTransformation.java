package org.apache.kafka.connect.transforms;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;

/**
 * base transformation
 * @param <R>
 */
public abstract class BaseTransformation<R extends ConnectRecord> implements Transform<R> {

    protected KeyValue config;
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
    public void start(KeyValue config) {
        this.config = config;
        this.validate(config);
    }

}
