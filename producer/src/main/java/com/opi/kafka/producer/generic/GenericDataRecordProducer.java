package com.opi.kafka.producer.generic;

import com.opi.kafka.producer.Producer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class GenericDataRecordProducer implements Producer {

    private final KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final String topicName;
    private final int batchSize;
    private GenericDataRecordGenerator dataGenerator = new GenericDataRecordGenerator();
    private final AtomicInteger counter = new AtomicInteger(0);

    public GenericDataRecordProducer(KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate,
                                     Schema keySchema,
                                     Schema valueSchema,
                                     String topicName,
                                     int batchSize) {
        this.kafkaTemplate = kafkaTemplate;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.topicName = topicName;
        this.batchSize = batchSize;
    }

    public void setDataGenerator(GenericDataRecordGenerator dataGenerator) {
        this.dataGenerator = dataGenerator;
    }

    @Override
    public void send() {

        // generate a batch of key/value pairs to send to the output topic
        List<KeyValue<GenericData.Record, GenericData.Record>> records = dataGenerator.generateBatch(batchSize, keySchema, valueSchema);
        records.forEach(kv -> {
            this.kafkaTemplate.send(this.topicName, kv.key, kv.value);
            int count = counter.incrementAndGet();
            if (count % 500 == 0) {
                log.info("Producer \"{}\" sent {} records, total = {}", this.topicName, 500, count);
            }
        });
    }
}
