package com.opi.kafka;

import com.opi.kafka.avro.Person;
import com.opi.kafka.avro.PersonKey;
import org.apache.avro.generic.GenericData;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.net.URISyntaxException;

@Configuration
public class Config {

    @Bean
    public SchemaLoader schemaLoader() {
        return new SchemaLoader();
    }

    @Bean
    @ConditionalOnProperty(value = "producers.person.enabled", matchIfMissing = false, havingValue = "true")
    public PersonProducer personProducer(KafkaTemplate<PersonKey, Person> kafkaTemplate) {
        return new PersonProducer(kafkaTemplate);
    }

    @Bean
    @ConditionalOnProperty(value = "producers.generic.enabled", matchIfMissing = false, havingValue = "true")
    public GenericRecordProducer genericRecordProducer(KafkaTemplate<GenericData.Record, GenericData.Record> kafkaTemplate,
                                                       @Value("${producers.generic.keySchema}") String keySchemaFilepath,
                                                       @Value("${producers.generic.valueSchema}") String valueSchemaFilepath)
            throws IOException, URISyntaxException {

        SchemaLoader loader = schemaLoader();
        return new GenericRecordProducer(kafkaTemplate, loader.getSchema(keySchemaFilepath), loader.getSchema(valueSchemaFilepath));
    }
}