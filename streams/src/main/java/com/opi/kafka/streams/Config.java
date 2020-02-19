package com.opi.kafka.streams;

import com.opi.kafka.streams.generic.GenericDataRecordStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
public class Config {

    @Value("${task.pool.size:100}")
    private int taskPoolSize;

    @Bean
    @ConfigurationProperties(prefix = "spring.kafka.streams")
    public Properties kafkaProperties() {
        return new Properties();
    }

    @Bean
    @ConfigurationProperties(prefix = "streams")
    public Properties streamProperties() {
        return new Properties();
    }

    @Bean(destroyMethod = "shutdown")
    public Executor executor() {
        return Executors.newScheduledThreadPool(taskPoolSize);
    }

    @Bean
    public ApplicationRunner runner(KafkaListenerEndpointRegistry registry, GenericApplicationContext context) {
        return args -> {

            List<GenericDataRecordStream> streams = new ArrayList<>();
            Properties kafkaProps = kafkaProperties();
            Executor executor = executor();
            Map<String, Map<String, String>> streamsProps = toMap(streamProperties());

            for (Map.Entry<String, Map<String, String>> entry : streamsProps.entrySet()) {
                Map<String, String> value = entry.getValue();
                GenericDataRecordStream stream = new GenericDataRecordStream(entry.getKey(), value.get("inputTopic"),
                        value.get("outputTopic"), value.get("keySchema"), value.get("valueSchema"), kafkaProps);
                streams.add(stream);
                executor.execute(stream::start);
            }

            // gracefully handles ctrl-c kills of application
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    streams.forEach(GenericDataRecordStream::close);
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