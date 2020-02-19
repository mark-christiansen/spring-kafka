package com.opi.kafka.producer.generic;

import com.github.javafaker.Faker;
import com.opi.kafka.producer.UuidUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.kafka.streams.KeyValue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class GenericDataRecordGenerator {

    private final Faker faker = new Faker();

    public List<KeyValue<GenericData.Record, GenericData.Record>> generateBatch(int batchSize, Schema keySchema, Schema valueSchema) {

        List<KeyValue<GenericData.Record, GenericData.Record>> keyValues = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {

            GenericData.Record key = generate(keySchema);
            GenericData.Record value = generate(valueSchema);
            // copy key fields values from key record over to value record so they match
            keySchema.getFields().forEach(f -> {
                value.put(f.name(), key.get(f.name()));
            });
            keyValues.add(new KeyValue<>(key, value));
        }
        return keyValues;
    }

    public GenericData.Record generate(Schema schema) {

        GenericData.Record record  = new GenericData.Record(schema);
        schema.getFields().forEach(f -> {

            Schema fieldSchema = f.schema();
            // if field is type union then grab the first non-"null" type as field type
            if (fieldSchema.getType().getName().equals("union")) {
                Optional<Schema> unionType = fieldSchema.getTypes().stream()
                        .filter(t -> !t.getType().getName().equals("null")).findFirst();
                if (!unionType.isPresent()) {
                    throw new RuntimeException(format("field \"%s\" union type does not have a non-null type", f.name()));
                }
                fieldSchema = unionType.get();
            }

            // field is not a logical type
            if (fieldSchema.getLogicalType() != null) {

                switch (fieldSchema.getLogicalType().getName()) {
                    case "decimal":
                        int scale  = (Integer) fieldSchema.getObjectProps().get("scale");
                        int precision = (Integer) fieldSchema.getObjectProps().get("precision");
                        record.put(f.name(), createBigDecimal(scale, precision));
                        break;
                    case "timestamp-micros":
                        record.put(f.name(), faker.date().past(365, TimeUnit.DAYS).toInstant());
                        break;
                    default:
                        throw new RuntimeException(format("Field \"%s\" logical type \"%s\" unknown", f.name(), fieldSchema.getLogicalType().getName()));
                }

            } else {

                switch (fieldSchema.getType().getName()) {
                    case "bytes":
                        record.put(f.name(), faker.letterify("??????").getBytes());
                        break;
                    case "enum":
                        List<String> symbols = fieldSchema.getEnumSymbols();
                        int selected = faker.number().numberBetween(0, symbols.size() - 1);
                        record.put(f.name(), new GenericData.EnumSymbol(fieldSchema, symbols.get(selected)));
                        break;
                    // using "fixed" 16 byte array for UUIDs
                    case "fixed":
                        record.put(f.name(), new GenericData.Fixed(fieldSchema, UuidUtil.toBytes(UUID.randomUUID())));
                        break;
                    case "int":
                        if (f.name().toUpperCase().contains("AGE")) {
                            record.put(f.name(), faker.number().numberBetween(1, 123));
                        } else {
                            record.put(f.name(), faker.number().numberBetween(1, Integer.MAX_VALUE));
                        }
                        break;
                    case "long":
                        record.put(f.name(), faker.number().numberBetween(1L, Long.MAX_VALUE));
                        break;
                    case "string":
                        if (f.name().toUpperCase().contains("ADDRESS")) {
                            record.put(f.name(), faker.address().fullAddress());
                        } else if (f.name().toUpperCase().contains("NAME")) {
                            record.put(f.name(), faker.name().fullName());
                        } else {
                            record.put(f.name(), faker.letterify("??????????"));
                        }
                        break;
                    default:
                        throw new RuntimeException(format("Field \"%s\" type \"%s\" unknown", f.name(), fieldSchema.getType().getName()));
                }
            }
        });
        return record;
    }

    private BigDecimal createBigDecimal(int scale, int precision) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < precision - scale; i++) {
            sb.append("#");
        }
        sb.append(".");
        for (int i = 0; i < scale; i++) {
            sb.append("#");
        }
        return new BigDecimal(faker.numerify(sb.toString()));
    }
}