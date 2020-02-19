package com.opi.kafka.producer.specific;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;

public interface SpecificDataGenerator {
    KeyValue<SpecificRecord, SpecificRecord> generate();
}