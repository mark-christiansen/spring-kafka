package com.opi.kafka.consumer.account;

import com.opi.kafka.consumer.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
public class AccountConsumer implements Consumer<GenericData.Record, GenericData.Record> {

    private AccountService accountService;
    private AtomicInteger counter = new AtomicInteger(0);

    public void setAccountService(AccountService accountService) {
        this.accountService = accountService;
    }

    @Override
    @KafkaListener(id = "${id}", clientIdPrefix = "${clientIdPrefix}", topics = "${topics}", groupId = "${groupId}")
    public void listen(List<ConsumerRecord<GenericData.Record, GenericData.Record>> records, Acknowledgment ack) {

        if (records.isEmpty()) {
            return;
        }

        for (ConsumerRecord<GenericData.Record, GenericData.Record> record : records) {
            ack.acknowledge();
            counter.incrementAndGet();
        }

        UUID breadCrumbId = UUID.randomUUID();

        // write accounts to database
        List<GenericRecord> accounts = records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        long startTime = System.currentTimeMillis();
        accountService.append(accounts, breadCrumbId);
        log.info("Consumer on thread {} saved records in {} ms", breadCrumbId, System.currentTimeMillis() - startTime);

        String schemaName = records.get(0).value().getSchema().getName();
        log.info("Consumer \"{}\" received {} records, total = {}", schemaName, records.size(), counter.get());
    }
}
