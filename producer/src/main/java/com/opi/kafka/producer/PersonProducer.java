package com.opi.kafka.producer;

import com.github.javafaker.Faker;
import com.opi.kafka.streams.avro.Person;
import com.opi.kafka.streams.avro.PersonKey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class PersonProducer {

    @Value("${topics.people}")
    private String topicName;

    private final KafkaTemplate<PersonKey, Person> kafkaTemplate;

    public PersonProducer(KafkaTemplate<PersonKey, Person> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 5)
    public void send() {
        Person person = createPerson();
        PersonKey key = new PersonKey();
        key.setName(person.getName());
        this.kafkaTemplate.send(this.topicName, key, person);
        log.info(String.format("Produced person -> %s", person));
    }

    private Person createPerson() {
        Faker faker = new Faker();
        Person person = new Person();
        person.setName(faker.name().fullName());
        person.setAddress(faker.address().fullAddress());
        person.setAge(faker.number().randomDigitNotZero());
        return person;
    }
}