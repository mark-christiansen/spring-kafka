package com.opi.kafka.consumer.generic;

import com.opi.kafka.consumer.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GenericDataRecordConsumer implements Consumer<GenericData.Record, GenericData.Record> {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    @KafkaListener(id = "${id}", clientIdPrefix = "${clientIdPrefix}", topics = "${topics}", groupId = "${groupId}")
    public void listen(List<ConsumerRecord<GenericData.Record, GenericData.Record>> records, Acknowledgment ack) {

        if (records.isEmpty()) {
            return;
        }

        for (ConsumerRecord<GenericData.Record, GenericData.Record> record : records) {
            ack.acknowledge();
            counter.incrementAndGet();
        }

        String schemaName = records.get(0).value().getSchema().getName();
        log.info("Consumer \"{}\" received {} records, total = {}", schemaName, records.size(), counter.get());
    }
}