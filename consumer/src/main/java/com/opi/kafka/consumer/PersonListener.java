package com.opi.kafka.consumer;

import com.opi.kafka.streams.avro.Person;
import com.opi.kafka.streams.avro.PersonKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

@Slf4j
public class PersonListener {

    private static final long WAIT = 0;

    @KafkaListener(topics = "${topics.people}")
    public void listen(List<ConsumerRecord<PersonKey, Person>> records, Acknowledgment ack) {

        log.info("received {} persons", records.size());
        for (ConsumerRecord<PersonKey, Person> record : records) {

            //log.info("person received: key = {}, value = {}}", record.key(), record.value());
            // do stuff
            try {
                Thread.sleep(WAIT);
            } catch (InterruptedException ignored) {
            }
            ack.acknowledge();
            //log.info("message acknowledged after {} ms", WAIT);
        }
    }
}