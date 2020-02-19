package com.opi.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public interface Consumer<K, V> {
    void listen(List<ConsumerRecord<K, V>> records, Acknowledgment ack);
}
