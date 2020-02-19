package com.opi.kafka.streams.generic;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class EncryptTransformer implements Transformer<GenericData.Record, GenericData.Record, KeyValue<GenericData.Record, GenericData.Record>> {

    private final Schema keySchema;
    private final Schema valueSchema;
    private final AesCipher aes;

    public EncryptTransformer(String secret, Schema keySchema, Schema valueSchema) {
        this.keySchema = keySchema;
        this.valueSchema = valueSchema;
        aes = new AesCipher(secret);
    }

    @Override
    public KeyValue<GenericData.Record, GenericData.Record> transform(GenericData.Record key, GenericData.Record value) {

        GenericData.Record encryptedKey = new GenericData.Record(keySchema);
        key.getSchema().getFields().forEach(f -> {
            Object val = key.get(f.name());
            if (val instanceof String) {
                encryptedKey.put(f.name(), aes.encrypt((String) val));
            } else {
                encryptedKey.put(f.name(), val);
            }
        });

        GenericData.Record encryptedValue = new GenericData.Record(valueSchema);
        value.getSchema().getFields().forEach(f -> {
            Object val = value.get(f.name());
            if (val instanceof String){
                encryptedValue.put(f.name(), aes.encrypt((String) val));
            } else if (val instanceof Integer) {
                encryptedValue.put(f.name(), aes.encrypt(((Integer)val).toString()));
            }  else {
                encryptedValue.put(f.name(), val);
            }
        });
        return new KeyValue<>(encryptedKey, encryptedValue);
    }

    @Override
    public void init(ProcessorContext context) {
    }

    @Override
    public void close() {
    }
}