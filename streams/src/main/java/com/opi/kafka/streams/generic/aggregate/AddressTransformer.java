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

public class AddressTransformer implements Transformer<GenericRecord, GenericRecord, KeyValue<GenericRecord, GenericRecord>> {

    private final String inputTopic;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final DeadLetterHandler deadLetterHandler;

    public AddressTransformer(String inputTopic, Schema keySchema, Schema valueSchema, DeadLetterHandler deadLetterHandler) {
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