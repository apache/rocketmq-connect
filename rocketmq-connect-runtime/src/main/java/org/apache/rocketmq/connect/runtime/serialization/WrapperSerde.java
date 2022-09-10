package org.apache.rocketmq.connect.runtime.serialization;

import java.util.Map;

public class WrapperSerde<T> implements Serde<T> {
    final private Serializer<T> serializer;
    final private Deserializer<T> deserializer;

    public WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        serializer.configure(configs);
        deserializer.configure(configs);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}