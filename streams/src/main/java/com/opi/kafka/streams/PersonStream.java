package com.opi.kafka.streams;

import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PersonStream {

    private String inputTopic;
    private String outputTopic;
    private Properties kafkaStreamsProperties;
    private KafkaStreams streams;
    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    public PersonStream(@Value("${topics.inputTopic}") String inputTopic,
                        @Value("${topics.inputTopic}") String outputTopic,
                        @Qualifier("kafkaStreamsProperties") Properties kafkaStreamsProperties) {

        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.kafkaStreamsProperties = kafkaStreamsProperties;
        setup();
        start();
    }

    private void setup() {

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic)
                .transform(UppercaseTransformer::new)
                .to(outputTopic);

        final Topology topology = builder.build();
        this.streams = new KafkaStreams(topology, kafkaStreamsProperties);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
    }

    private int start() {
        try {
            streams.close();
            latch.await();
        } catch (Throwable t) {
            return 1;
        }
        streams.close();
        return 0;
    }
}

class UppercaseTransformer implements Transformer<Object, Object, KeyValue<Object, Object>> {

    @Override
    public KeyValue<Object, Object> transform(Object key, Object value) {

        GenericData.Record keyRecord  = (GenericData.Record) key;
        keyRecord.getSchema().getFields().forEach(f -> {
            Object val = keyRecord.get(f.name());
            if (val != null && val instanceof String) {
                keyRecord.put(f.name(), ((String) val).toUpperCase());
            }
        });

        GenericData.Record valueRecord  = (GenericData.Record) value;
        valueRecord.getSchema().getFields().forEach(f -> {
            Object val = valueRecord.get(f.name());
            if (val != null && val instanceof String) {
                valueRecord.put(f.name(), ((String) val).toUpperCase());
            }
        });
        return new KeyValue(keyRecord, valueRecord);
    }

    @Override
    public void init(ProcessorContext context) {
    }

    @Override
    public void close() {
    }
}