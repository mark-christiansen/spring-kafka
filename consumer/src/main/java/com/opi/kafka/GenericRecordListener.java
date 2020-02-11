package com.opi.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

@Slf4j
public class GenericRecordListener {

    private static final long WAIT = 0;

    @KafkaListener(topics = "${topics.people}")
    public void listen(List<ConsumerRecord<GenericData.Record, GenericData.Record>> records, Acknowledgment ack) {

        log.info("received {} records", records.size());
        for (ConsumerRecord<GenericData.Record, GenericData.Record> record : records) {

            //log.info("record received: key = {}, value = {}}", record.key(), record.value());
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