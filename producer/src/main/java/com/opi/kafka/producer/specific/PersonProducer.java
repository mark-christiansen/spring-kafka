package com.opi.kafka.producer.specific;

import com.opi.kafka.streams.avro.Person;
import com.opi.kafka.streams.avro.PersonKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PersonProducer {

    private final KafkaTemplate<PersonKey, Person> kafkaTemplate;
    private final String topicName;
    private final PersonGenerator personGenerator = new PersonGenerator();
    private final AtomicInteger counter = new AtomicInteger(0);

    public PersonProducer(KafkaTemplate<PersonKey, Person> kafkaTemplate, String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void send() {
        KeyValue<PersonKey, Person> keyValue = personGenerator.create();
        this.kafkaTemplate.send(this.topicName, keyValue.key, keyValue.value);
        int count = counter.incrementAndGet();
        if (count % 500 == 0) {
            log.info("Producer \"{}\" sent {} records, total = {}", this.topicName, 500, count);
        }
    }
}