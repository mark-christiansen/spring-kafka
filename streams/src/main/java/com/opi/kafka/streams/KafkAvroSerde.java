package com.opi.kafka.streams;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkAvroSerde implements Serde {

    private Serde inner;

    public KafkAvroSerde() {
        this.inner = Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
    }

    @Override
    public void configure(Map configs, boolean isKey) {
        this.inner.configure(configs, isKey);
    }

    @Override
    public void close() {
        this.inner.close();
    }

    @Override
    public Serializer serializer() {
        return this.inner.serializer();
    }

    @Override
    public Deserializer deserializer() {
        return this.inner.deserializer();
    }
}