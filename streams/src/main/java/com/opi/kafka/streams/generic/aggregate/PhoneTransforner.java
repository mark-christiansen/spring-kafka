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

public class PhoneTransforner implements Transformer<GenericRecord, GenericRecord, KeyValue<GenericRecord, GenericRecord>> {

    private final String inputTopic;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final DeadLetterHandler deadLetterHandler;

    public PhoneTransforner(String inputTopic, Schema keySchema, Schema valueSchema, DeadLetterHandler deadLetterHandler) {
        this.inputTopic = inputTopic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.deadLetterHandler = deadLetterHandler;
    }

    @Override
    public KeyValue<GenericRecord, GenericRecord> transform(GenericRecord key, GenericRecord value) {

        try {
            GenericData.Record newKey = new GenericData.Record(keySchema);
            newKey.put("id", value.get("personId"));

            GenericRecord newValue = new GenericData.Record(valueSchema);

            Schema phoneSchema = valueSchema.getField("phones").schema().getTypes().get(1).getElementType();
            GenericData.Record phone = new GenericData.Record(phoneSchema);
            phone.put("id", value.get("id"));
            phone.put("personId", value.get("personId"));
            phone.put("type", value.get("type"));
            phone.put("areaCode", value.get("areaCode"));
            phone.put("number", value.get("number"));
            phone.put("extension", value.get("extension"));

            newValue.put("id", value.get("personId"));
            final List<GenericRecord> phones = new ArrayList<>();
            phones.add(phone);
            newValue.put("phones", phones);

            return new KeyValue<>(newKey, newValue);

        } catch (Exception e) {
            // send message to dead-letter queue for this input topic if an exception occurs during transformation
            deadLetterHandler.handle(this.inputTopic, new KeyValue(key, value));
        }
        return null;
    }

    @Override
    public void init(ProcessorContext context) {
    }

    @Override
    public void close() {
    }
}