package com.opi.kafka.producer.generic;

import com.github.javafaker.Faker;
import com.opi.kafka.producer.UuidUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KeyValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class GenericDataRecordGenerator {

    private final Faker faker = new Faker();
    private final List<Object> primaryKeys = new ArrayList<>();
    private final Set<String> primaryKeyFields = new HashSet<>();
    private final Map<String, String> foreignKeyFields = new HashMap<>();
    private int maxPrimaryKeys;

    public void addPrimaryKey(String primaryKey) {
        primaryKeyFields.add(primaryKey);
    }

    public void addForeignKey(String foreignKey, String primaryKey) {
        foreignKeyFields.put(foreignKey, primaryKey);
    }

    public void setMaxPrimaryKey(int maxPrimaryKeys) {
        this.maxPrimaryKeys = maxPrimaryKeys;
    }

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
                        record.put(f.name(), Instant.ofEpochMilli(faker.number().numberBetween(0, System.currentTimeMillis())));
                        break;
                    case "timestamp-millis":
                        record.put(f.name(), Instant.ofEpochMilli(faker.number().numberBetween(0, System.currentTimeMillis())));
                        break;
                    case "date":
                        record.put(f.name(), faker.date().past(365, TimeUnit.DAYS));
                        break;
                    default:
                        throw new RuntimeException(format("Field \"%s\" logical type \"%s\" unknown", f.name(), fieldSchema.getLogicalType().getName()));
                }

            } else {

                switch (fieldSchema.getType().getName()) {
                    case "bytes":
                        record.put(f.name(), ByteBuffer.wrap(faker.letterify("??????").getBytes()));
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
                    case "double":
                        record.put(f.name(), faker.number().randomDouble(2, 1L, Long.MAX_VALUE));
                        break;
                    case "float":
                        record.put(f.name(), (float) faker.number().randomDouble(4, 1, 1000));
                        break;
                    case "boolean":
                        record.put(f.name(), faker.bool().bool());
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

            String fullFieldName = schema.getName() + "." + f.name();

            if (primaryKeyFields.contains(fullFieldName)) {
                if (primaryKeys.size() < maxPrimaryKeys) {
                    primaryKeys.add(record.get(f.name()));
                } else {
                    // randomly pick a primary key from the list
                    int index = faker.number().numberBetween(0, primaryKeys.size()-1);
                    record.put(f.name(), primaryKeys.get(index));
                }
            }

            if (foreignKeyFields.containsKey(fullFieldName)) {
                if (primaryKeys.size() > maxPrimaryKeys/4) {
                    // randomly pick a primary key from the list
                    int index = faker.number().numberBetween(0, primaryKeys.size() - 1);
                    record.put(f.name(), primaryKeys.get(index));
                } else {
                    primaryKeys.add(record.get(f.name()));
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