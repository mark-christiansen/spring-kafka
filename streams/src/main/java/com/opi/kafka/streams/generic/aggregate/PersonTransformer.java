package com.opi.kafka.streams.generic.aggregate;

import com.opi.kafka.streams.error.DeadLetterHandler;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class PersonTransformer implements Transformer<GenericRecord, GenericRecord, KeyValue<GenericRecord, GenericRecord>> {

    private final String inputTopic;
    private final Schema keySchema;
    private final Schema valueSchema;
    private final DeadLetterHandler deadLetterHandler;

    public PersonTransformer(String inputTopic, Schema keySchema, Schema valueSchema, DeadLetterHandler deadLetterHandler) {
        this.inputTopic = inputTopic;
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        this.deadLetterHandler = deadLetterHandler;
    }

    @Override
    public KeyValue<GenericRecord, GenericRecord> transform(GenericRecord key, GenericRecord value) {

        try {
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