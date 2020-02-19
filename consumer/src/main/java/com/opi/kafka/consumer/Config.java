package com.opi.kafka.consumer;

import com.opi.kafka.consumer.generic.GenericDataRecordConsumer;
import com.opi.kafka.consumer.specific.PersonConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.SeekToCurrentBatchErrorHandler;
import org.springframework.kafka.support.LogIfLevelEnabled;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static java.lang.Class.forName;
import static java.lang.String.format;

@Configuration
public class Config {

    @Autowired
    private ApplicationContext context;
    @Value("${spring.kafka.consumer.concurrency:10}")
    private int concurrency;

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
        factory.setConcurrency(concurrency);
        factory.setBatchErrorHandler(new SeekToCurrentBatchErrorHandler());
        configurer.configure(factory, kafkaConsumerFactory);
        return factory;
    }

    @Bean("consumerProperties")
    @ConfigurationProperties(prefix = "consumers")
    public Properties consumerProperties() {
        return new Properties();
    }

    @Bean
    public ApplicationRunner runner(KafkaListenerEndpointRegistry registry, GenericApplicationContext context) {
        return args -> {
            Map<String, Map<String, String>> consumerProps = toMap(consumerProperties());
            for (Map.Entry<String, Map<String, String>> entry : consumerProps.entrySet()) {

                String key = entry.getKey();

                Properties props = new Properties();
                props.setProperty("id", key);
                props.setProperty("clientIdPrefix", format("spring-%s-client", key));
                props.setProperty("groupId", format("spring-%s-group", key));
                props.setProperty("topics", entry.getValue().get("inputTopic"));
                PropertiesPropertySource source = new PropertiesPropertySource("dynamicListenerId", props);

                context.getEnvironment().getPropertySources().addLast(source);
                if (entry.getValue().get("type").equals("generic")) {
                    context.registerBean(key, GenericDataRecordConsumer.class, GenericDataRecordConsumer::new);
                } else {
                    Class consumerClass = forName(entry.getValue().get("consumerClass"));
                    context.registerBean(key, consumerClass);
                }
                context.getBean(key);
            }
            registry.getListenerContainerIds().forEach(System.out::println);

            // gracefully handles ctrl-c kills of application
            Runtime.getRuntime().addShutdownHook(new Thread("consumer-shutdown-hook") {
                @Override
                public void run() {
                    registry.destroy();
                }
            });
        };
    }

    private Map<String, Map<String, String>> toMap(Properties props) {

        Map<String, Map<String, String>> map = new HashMap<>();
        props.stringPropertyNames().forEach(name -> {

            String[] nameParts = name.split("\\.");
            if (nameParts.length == 2) {
                String streamName = nameParts[0];
                Map<String, String> streamMap = map.computeIfAbsent(streamName, k -> new HashMap<>());
                streamMap.put(nameParts[1], (String) props.get(name));
            }
        });
        // remove all of the streams that are disabled
        map.entrySet().removeIf(entry -> !Boolean.parseBoolean(entry.getValue().get("enabled")));
        return map;
    }
}