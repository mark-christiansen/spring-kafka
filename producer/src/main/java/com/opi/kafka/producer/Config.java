package com.opi.kafka.producer;

import com.opi.kafka.producer.generic.GenericDataRecordProducer;
import com.opi.kafka.producer.specific.SpecificDataGenerator;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

@Configuration
@Slf4j
public class Config implements SchedulingConfigurer {

    @Autowired
    private ApplicationContext applicationContext;
    @Autowired
    private KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate;
    @Value("${schedule.send.rate.ms:10}")
    private int sendRate;
    @Value("${task.pool.size:100}")
    private int taskPoolSize;

    @Bean
    public SchemaLoader schemaLoader() {
        return new SchemaLoader();
    }

    @Bean(destroyMethod = "shutdown")
    public Executor taskExecutor() {
        return Executors.newScheduledThreadPool(taskPoolSize);
    }

    @Bean("producerProperties")
    @ConfigurationProperties(prefix = "producers")
    public Properties producerProperties() {
        return new Properties();
    }

    @Bean
    public List<Producer> producers() throws IOException, URISyntaxException {

        List<Producer> producers = new ArrayList<>();
        Map<String, Map<String, String>> producerProps = toMap(producerProperties());
        ConfigurableListableBeanFactory  beanFactory = ((ConfigurableApplicationContext) applicationContext).getBeanFactory();
        SchemaLoader schemaLoader = schemaLoader();

        for (Map.Entry<String, Map<String, String>> entry : producerProps.entrySet()) {

            Map<String, String> props = entry.getValue();
            String type = props.get("type");
            if (type.equals("generic")) {
                Schema keySchema = schemaLoader.getSchema(props.get("keySchema"));
                Schema valueSchema = schemaLoader.getSchema(props.get("valueSchema"));
                GenericDataRecordProducer producer = new GenericDataRecordProducer(kafkaTemplate, keySchema, valueSchema,
                        props.get("outputTopic"), Integer.parseInt(props.get("batchSize")));
                producers.add(producer);
            } else {
                try {
                    SpecificDataGenerator dataGenerator = (SpecificDataGenerator) Class.forName(props.get("dataGeneratorClass"))
                            .getDeclaredConstructor(Void.class).newInstance();
                    //SpecificRecordProducer producer = new SpecificRecordProducer(kafkaTemplate, props.get("outputTopic"), dataGenerator);
                    //producers.add(producer);
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
                    log.error("Error creating specific record producer", e);
                }
            }
        }
        return producers;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {

        try {
            List<Producer> producers = producers();

            taskRegistrar.setScheduler(taskExecutor());
            taskRegistrar.addTriggerTask(() -> producers.forEach(p -> p.send()),
                    triggerContext -> {
                        Calendar nextExecTime = new GregorianCalendar();
                        Date lastActualExecTime = triggerContext.lastActualExecutionTime();
                        nextExecTime.setTime(lastActualExecTime != null ? lastActualExecTime : new Date());
                        nextExecTime.add(Calendar.MILLISECOND, sendRate);
                        return nextExecTime.getTime();
                    }
            );

            // gracefully handles ctrl-c kills of application
            Runtime.getRuntime().addShutdownHook(new Thread("scheduler-shutdown-hook") {
                @Override
                public void run() {
                    taskRegistrar.destroy();
                }
            });

        } catch(IOException | URISyntaxException e) {
            log.error("Error occurred scheduling producers", e);
        }
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