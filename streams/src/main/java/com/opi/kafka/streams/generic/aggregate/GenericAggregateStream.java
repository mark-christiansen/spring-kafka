package com.opi.kafka.streams.generic.aggregate;

import com.opi.kafka.streams.error.DeadLetterHandler;
import com.opi.kafka.streams.generic.GenericStream;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

import static com.opi.kafka.streams.error.DeadLetterHandler.DEAD_LETTER_HANDLER;

@Slf4j
public class GenericAggregateStream extends GenericStream {

    public GenericAggregateStream(String applicationId,
                                  String[] inputTopics,
                                  String outputTopic,
                                  String[] keySchemaFilepath,
                                  String[] valueSchemaFilepath,
                                  Properties kafkaStreamsProperties) throws IOException, URISyntaxException {
        super(applicationId, inputTopics, new String[] {outputTopic}, keySchemaFilepath,
                valueSchemaFilepath, kafkaStreamsProperties);
    }

    @Override
    public StreamsBuilder streamsBuilder() {

        DeadLetterHandler deadLetterHandler = (DeadLetterHandler) kafkaStreamsProperties.get(DEAD_LETTER_HANDLER);
        GenericAvroSerde valueSerde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", kafkaStreamsProperties.getProperty("schema.registry.url"));
        valueSerde.configure(config, false);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<GenericRecord, GenericRecord> person = builder.stream(inputTopics[0]);
        KStream<GenericRecord, GenericRecord> address = builder.stream(inputTopics[1]);
        KStream<GenericRecord, GenericRecord> phone = builder.stream(inputTopics[2]);

        final KGroupedStream<GenericRecord, GenericRecord> groupedStream = person.merge(address)
                .merge(phone)
                .transform(() -> new AccountTransformer(inputTopics, keySchemas[0], valueSchemas[0], deadLetterHandler))
                .through(outputTopics[0] + "-transform")
                .groupByKey();
        final KTable<GenericRecord, GenericRecord> aggregate = groupedStream.aggregate(
                () -> new GenericData.Record(valueSchemas[0]),
                new AccountAggregator(inputTopics[0], deadLetterHandler),
                Materialized.<GenericRecord, GenericRecord, KeyValueStore<Bytes, byte[]>>as("account-stream-store").withValueSerde(valueSerde).withCachingEnabled());
        aggregate.toStream().to(outputTopics[0]);
        return builder;
    }
}