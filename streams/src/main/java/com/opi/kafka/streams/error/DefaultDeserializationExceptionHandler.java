package com.opi.kafka.streams.error;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Map;

import static com.opi.kafka.streams.error.DeadLetterHandler.DEAD_LETTER_HANDLER;
import static java.lang.String.format;

@Slf4j
public class DefaultDeserializationExceptionHandler implements DeserializationExceptionHandler {

    private DeadLetterHandler deadLetterHandler;

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        log.error(format("Error occurred consuming record: key = \"%s\"", new String(record.key())), exception);
        if (deadLetterHandler != null) {
            deadLetterHandler.handle(context.topic(), new KeyValue(record.key(), record.value()));
        }
        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object obj = configs.get(DEAD_LETTER_HANDLER);
        if (obj instanceof DeadLetterHandler) {
            this.deadLetterHandler = (DeadLetterHandler) obj;
        }
    }
}