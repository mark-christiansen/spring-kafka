package com.opi.kafka.streams.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;

import java.util.Map;

import static com.opi.kafka.streams.error.DeadLetterHandler.DEAD_LETTER_HANDLER;
import static java.lang.String.format;

@Slf4j
public class DefaultProductionExceptionHandler implements ProductionExceptionHandler {

    private DeadLetterHandler deadLetterHandler;

    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        log.error(format("Error occurred producing record: key = \"%s\"", new String(record.key())), exception);
        if (deadLetterHandler != null) {
            deadLetterHandler.handle(record.topic(), new KeyValue(record.key(), record.value()));
        }
        return ProductionExceptionHandler.ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object obj = configs.get(DEAD_LETTER_HANDLER);
        if (obj instanceof DeadLetterHandler) {
            this.deadLetterHandler = (DeadLetterHandler) obj;
        }
    }
}