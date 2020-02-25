package com.opi.kafka.consumer.specific;

import com.opi.kafka.consumer.Consumer;
import com.opi.kafka.avro.Person;
import com.opi.kafka.avro.PersonKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class PersonConsumer implements Consumer<PersonKey, Person> {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    @KafkaListener(id = "${id}", clientIdPrefix = "${clientIdPrefix}", topics = "${topics}", groupId = "${groupId}")
    public void listen(List<ConsumerRecord<PersonKey, Person>> records, Acknowledgment ack) {

        if (records.isEmpty()) {
            return;
        }

        for (ConsumerRecord<PersonKey, Person> record : records) {
            ack.acknowledge();
            counter.incrementAndGet();
        }

        String schemaName = records.get(0).value().getSchema().getName();
        log.info("Consumer \"{}\" received {} records, total = {}", schemaName, records.size(), counter.get());
    }
}