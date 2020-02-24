package com.opi.kafka.streams.generic.aggregate;

import com.opi.kafka.streams.error.DeadLetterHandler;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class AccountTransformer implements Transformer<GenericRecord, GenericRecord, KeyValue<GenericRecord, GenericRecord>> {

    private final String[] inputTopics;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final DeadLetterHandler deadLetterHandler;

    public AccountTransformer(String[] inputTopics, Schema keySchema, Schema valueSchema, DeadLetterHandler deadLetterHandler) {
        this.inputTopics = inputTopics;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.deadLetterHandler = deadLetterHandler;
    }

    @Override
    public KeyValue<GenericRecord, GenericRecord> transform(GenericRecord key, GenericRecord value) {

        String inputTopic = null;
        try {
            String type = value.getSchema().getName();
            switch (type) {

                case "Person":
                    inputTopic = inputTopics[0];
                    return transformPerson(key, value);
                case "Address":
                    inputTopic = inputTopics[1];
                    return transformAddress(key, value);
                case "Phone":
                    inputTopic = inputTopics[2];
                    return transformPhone(key, value);
                default:
                    inputTopic = "unknown";
                    throw new Exception(format("Received value record with unexpected type \"%s\" in account transformer", type));
            }
        } catch (Exception e) {
            // send message to dead-letter queue for this input topic if an exception occurs during transformation
            deadLetterHandler.handle(inputTopic, new KeyValue(key, value));
        }
        return null;
    }

    private KeyValue<GenericRecord, GenericRecord> transformPerson(GenericRecord key, GenericRecord value) {

        GenericData.Record newKey = new GenericData.Record(keySchema);
        newKey.put("id", value.get("id"));

        GenericRecord newValue = new GenericData.Record(valueSchema);
        newValue.put("id", value.get("id"));
        newValue.put("firstName", value.get("firstName"));
        newValue.put("middleName", value.get("middleName"));
        newValue.put("lastName", value.get("lastName"));
        newValue.put("birthDate", value.get("birthDate"));
        newValue.put("salary", value.get("salary"));

        return new KeyValue<>(newKey, newValue);
    }

    private KeyValue<GenericRecord, GenericRecord> transformAddress(GenericRecord key, GenericRecord value) {

        GenericData.Record newKey = new GenericData.Record(keySchema);
        newKey.put("id", value.get("personId"));

        Schema addressSchema = valueSchema.getField("addresses").schema().getTypes().get(1).getElementType();
        GenericData.Record address = new GenericData.Record(addressSchema);
        address.put("id", value.get("id"));
        address.put("personId", value.get("personId"));
        address.put("line1", value.get("line1"));
        address.put("line2", value.get("line2"));
        address.put("line3", value.get("line3"));
        address.put("city", value.get("city"));
        address.put("state", value.get("state"));
        address.put("country", value.get("country"));
        address.put("postalCode", value.get("postalCode"));

        GenericRecord newValue = new GenericData.Record(valueSchema);
        newValue.put("id", value.get("personId"));
        final List<GenericRecord> addresses = new ArrayList<>();
        addresses.add(address);
        newValue.put("addresses", addresses);

        return new KeyValue<>(newKey, newValue);
    }

    private KeyValue<GenericRecord, GenericRecord> transformPhone(GenericRecord key, GenericRecord value) {

        GenericData.Record newKey = new GenericData.Record(keySchema);
        newKey.put("id", value.get("personId"));

        Schema phoneSchema = valueSchema.getField("phones").schema().getTypes().get(1).getElementType();
        GenericData.Record phone = new GenericData.Record(phoneSchema);
        phone.put("id", value.get("id"));
        phone.put("personId", value.get("personId"));
        phone.put("type", value.get("type"));
        phone.put("areaCode", value.get("areaCode"));
        phone.put("number", value.get("number"));
        phone.put("extension", value.get("extension"));

        GenericRecord newValue = new GenericData.Record(valueSchema);
        newValue.put("id", value.get("personId"));
        final List<GenericRecord> phones = new ArrayList<>();
        phones.add(phone);
        newValue.put("phones", phones);

        return new KeyValue<>(newKey, newValue);
    }

    @Override
    public void init(ProcessorContext context) {
    }

    @Override
    public void close() {
    }
}
