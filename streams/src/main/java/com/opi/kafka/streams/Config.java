package com.opi.kafka.streams;

import com.opi.kafka.streams.error.DeadLetterHandler;
import com.opi.kafka.streams.generic.GenericStream;
import com.opi.kafka.streams.generic.aggregate.GenericAggregateStream;
import com.opi.kafka.streams.generic.transform.GenericTransformStream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.opi.kafka.streams.error.DeadLetterHandler.DEAD_LETTER_HANDLER;

@Configuration
@Slf4j
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
    public ApplicationRunner runner(KafkaListenerEndpointRegistry registry, GenericApplicationContext context, KafkaTemplate kafkaTemplate) {
        return args -> {

            List<GenericStream> streams = new ArrayList<>();
            Executor executor = executor();
            Map<String, Map<String, String>> streamsProps = toMap(streamProperties());

            // Get the Kafka settings and add the dead letter handler to the settings for the exception handlers to use
            // for sending poison pill messages to the dead letter queue.
            Properties kafkaProps = kafkaProperties();
            DeadLetterHandler deadLetterHandler = new DeadLetterHandler(kafkaTemplate);
            kafkaProps.put(DEAD_LETTER_HANDLER, deadLetterHandler);

            CountDownLatch latch = new CountDownLatch(streamsProps.size());

            for (Map.Entry<String, Map<String, String>> entry : streamsProps.entrySet()) {
                GenericStream stream = createGenericStream(kafkaProps.getProperty("application.id"), entry.getValue(), kafkaProps);
                streams.add(stream);
                stream.addListener(latch::countDown);
                executor.execute(stream::start);
            }

            // gracefully handles ctrl-c kills of application
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                @Override
                public void run() {
                    log.info("Shutdown hook activated");
                    streams.forEach(GenericStream::close);
                }
            });

            // start background thread to listen until all streams are stopped and then stop the Spring Boot application
            new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ignored) {}
                context.close();
            }).start();
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

    private GenericStream createGenericStream(String applicationId, Map<String, String> props, Properties kafkaProps)
            throws IOException, URISyntaxException {

        switch (props.get("type")) {
            case "generic-aggregate":

                String[] inputTopics = props.get("inputTopic").split(",");
                String[] keySchemas = props.get("keySchema").split(",");
                String[] valueSchemas = props.get("valueSchema").split(",");
                return new GenericAggregateStream(applicationId, inputTopics, props.get("outputTopic"), keySchemas, valueSchemas, kafkaProps);

            case "generic-transform":
                return new GenericTransformStream(applicationId, props.get("inputTopic"),
                        props.get("outputTopic"), props.get("keySchema"), props.get("valueSchema"), kafkaProps);
            default:
                return new GenericStream(applicationId, props.get("inputTopic"), props.get("outputTopic"),
                        props.get("keySchema"), props.get("valueSchema"), kafkaProps);

        }
    }
}