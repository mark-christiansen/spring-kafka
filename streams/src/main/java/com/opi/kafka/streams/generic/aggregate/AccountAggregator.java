package com.opi.kafka.streams.generic.aggregate;

import com.opi.kafka.streams.error.DeadLetterHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Aggregator;

import java.util.List;
import java.util.Optional;

@Slf4j
public class AccountAggregator implements Aggregator<GenericRecord, GenericRecord, GenericRecord> {

    private final String inputTopic;
    private final DeadLetterHandler deadLetterHandler;

    public AccountAggregator(String inputTopic, DeadLetterHandler deadLetterHandler) {
        this.inputTopic = inputTopic;
        this.deadLetterHandler = deadLetterHandler;
    }

    @Override
    public GenericRecord apply(GenericRecord key, GenericRecord value, GenericRecord aggregate) {

        try {

            if (value.get("addresses") != null) {
                mergeAddress(value, aggregate);
            } else if (value.get("phones") != null) {
                mergePhone(value, aggregate);
            } else {
                mergeAccount(value, aggregate);
            }

        } catch (Exception e) {
            // send message to dead-letter queue for this input topic if an exception occurs during aggregation
            deadLetterHandler.handle(this.inputTopic, new KeyValue(key, value));
        }

        return aggregate;
    }

    private void mergeAccount(GenericRecord src, GenericRecord dest) {
        dest.put("id", src.get("id"));
        dest.put("firstName", src.get("firstName"));
        dest.put("middleName", src.get("middleName"));
        dest.put("lastName", src.get("lastName"));
        dest.put("birthDate", src.get("birthDate"));
        dest.put("salary", src.get("salary"));
    }

    private void mergeAddress(GenericRecord src, GenericRecord dest) {

        dest.put("id", src.get("id"));

        List<GenericRecord> srcAddresses = (List<GenericRecord>) src.get("addresses");
        List<GenericRecord> destAddresses = (List<GenericRecord>) dest.get("addresses");

        if (destAddresses == null) {
            dest.put("addresses", srcAddresses);
        } else {

            srcAddresses.forEach(inAddress -> {
                Optional<GenericRecord> addressOptional = destAddresses.stream()
                        .filter(a -> a.get("id").equals(inAddress.get("id"))).findFirst();
                if (!addressOptional.isPresent()) {
                    destAddresses.add(inAddress);
                } else {
                    GenericRecord outAddress = addressOptional.get();
                    outAddress.put("personId", inAddress.get("personId"));
                    outAddress.put("line1", inAddress.get("line1"));
                    outAddress.put("line2", inAddress.get("line2"));
                    outAddress.put("line3", inAddress.get("line3"));
                    outAddress.put("city", inAddress.get("city"));
                    outAddress.put("state", inAddress.get("state"));
                    outAddress.put("country", inAddress.get("country"));
                    outAddress.put("postalCode", inAddress.get("postalCode"));
                }
            });
        }
    }

    private void mergePhone(GenericRecord src, GenericRecord dest) {

        dest.put("id", src.get("id"));

        List<GenericRecord> srcPhones = (List<GenericRecord>) src.get("phones");
        List<GenericRecord> destPhones = (List<GenericRecord>) dest.get("phones");

        if (destPhones == null) {
            dest.put("phones", srcPhones);
        } else {

            srcPhones.forEach(inPhone -> {
                Optional<GenericRecord> phoneOptional = destPhones.stream()
                        .filter(p -> p.get("id").equals(inPhone.get("id"))).findFirst();
                if (!phoneOptional.isPresent()) {
                    destPhones.add(inPhone);
                } else {
                    GenericRecord outPhone = phoneOptional.get();
                    outPhone.put("personId", inPhone.get("personId"));
                    outPhone.put("type", inPhone.get("type"));
                    outPhone.put("areaCode", inPhone.get("areaCode"));
                    outPhone.put("number", inPhone.get("number"));
                    outPhone.put("extension", inPhone.get("extension"));
                }
            });
        }
    }
}