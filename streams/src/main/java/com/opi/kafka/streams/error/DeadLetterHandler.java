package com.opi.kafka.streams.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.springframework.kafka.core.KafkaTemplate;

@Slf4j
public class DeadLetterHandler {

    public static final String DEAD_LETTER_HANDLER = "kafka.streams.dead.letter.handler";

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public DeadLetterHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void handle(String topicName, KeyValue<Object, Object> kv) {
        String dlqTopicName = topicName + ".dead";
        log.info("DLQ Producer \"{}\" sending record to DLQ", dlqTopicName);
        this.kafkaTemplate.send(dlqTopicName, kv.key, kv.value);
    }
}