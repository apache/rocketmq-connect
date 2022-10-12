/**
 *
 */
package org.apache.rocketmq.schema.common;

import io.openmessaging.connector.api.data.Schema;

import java.util.Map;

public interface Serializer<T> {

    default void configure(Map<String, ?> props){
    }

    /**
     * serialize data
     * @param topic
     * @param isKey
     * @param schema
     * @param value
     * @return
     */
    byte[] serialize(String topic, boolean isKey, T schema, Object value);
}
