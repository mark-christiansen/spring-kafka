package com.opi.kafka.producer.specific;

import com.opi.kafka.producer.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class SpecificRecordProducer implements Producer {

    private final KafkaTemplate<SpecificRecord, SpecificRecord> kafkaTemplate;
    private final String topicName;
    private final SpecificDataGenerator dataGenerator;
    private final AtomicInteger counter = new AtomicInteger(0);

    public SpecificRecordProducer(KafkaTemplate<SpecificRecord, SpecificRecord> kafkaTemplate, String topicName, SpecificDataGenerator dataGenerator) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
        this.dataGenerator = dataGenerator;
    }

    @Override
    public void send() {
        KeyValue<SpecificRecord, SpecificRecord> keyValue = dataGenerator.generate();
        this.kafkaTemplate.send(this.topicName, keyValue.key, keyValue.value);
        int count = counter.incrementAndGet();
        if (count % 500 == 0) {
            log.info("Producer \"{}\" sent {} records, total = {}", this.topicName, 500, count);
        }
    }
}
