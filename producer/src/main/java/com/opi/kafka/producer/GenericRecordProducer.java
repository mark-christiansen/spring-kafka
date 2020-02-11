package com.opi.kafka.producer;

import com.github.javafaker.Faker;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class GenericRecordProducer {

    @Value("${topics.people}")
    private String topicName;

    private final KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final Faker faker = new Faker();

    public GenericRecordProducer(KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate,
                                 Schema keySchema,
                                 Schema valueSchema) {
        this.kafkaTemplate = kafkaTemplate;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
    }

    @Scheduled(fixedDelay = 5)
    public void send() {
        GenericData.Record value = createRecord();
        GenericData.Record key = new GenericData.Record(keySchema);
        key.put("name", value.get("name"));
        this.kafkaTemplate.send(this.topicName, key, value);
        log.info(String.format("Produced generic record -> %s", value));
    }

    private GenericData.Record createRecord() {
        GenericData.Record record = new GenericData.Record(valueSchema);
        record.put("name", faker.name().fullName());
        record.put("address", faker.address().fullAddress());
        record.put("age", faker.number().numberBetween(5, 100));
        return record;
    }
}
