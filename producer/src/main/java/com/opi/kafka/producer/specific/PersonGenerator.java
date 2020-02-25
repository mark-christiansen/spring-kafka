package com.opi.kafka.producer.specific;

import com.github.javafaker.Faker;
import com.opi.kafka.avro.Person;
import com.opi.kafka.avro.PersonKey;
import org.apache.kafka.streams.KeyValue;

public class PersonGenerator {

    private final Faker faker = new Faker();

    public KeyValue<PersonKey, Person> create() {
        PersonKey key = new PersonKey();
        Person value = new Person();
        value.setName(faker.name().fullName());
        key.setName(value.getName());
        value.setAddress(faker.address().fullAddress());
        value.setAge(faker.number().randomDigitNotZero());
        return new KeyValue<>(key, value);
    }
}
