package com.opi.kafka;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.support.LogIfLevelEnabled;

@Configuration
public class Config {

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> kafkaConsumerFactory) {

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<Object, Object>();
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        // log commits at debug level
        factory.getContainerProperties().setCommitLogLevel(LogIfLevelEnabled.Level.DEBUG);
        // enable batching of commit offset messages
        factory.setBatchListener(true);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        configurer.configure(factory, kafkaConsumerFactory);
        return factory;
    }

    @Bean
    @ConditionalOnProperty(value = "consumers.person.enabled", matchIfMissing = false, havingValue = "true")
    public PersonListener personListener() {
        return new PersonListener();
    }

    @Bean
    @ConditionalOnProperty(value = "consumers.generic.enabled", matchIfMissing = false, havingValue = "true")
    public GenericRecordListener genericRecordListener() {
        return new GenericRecordListener();
    }
}