package com.opi.kafka.streams.generic.transform;

import com.opi.kafka.streams.error.DeadLetterHandler;
import com.opi.kafka.streams.generic.GenericStream;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TransformerSupplier;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import static com.opi.kafka.streams.error.DeadLetterHandler.DEAD_LETTER_HANDLER;

public class GenericTransformStream extends GenericStream {

    public GenericTransformStream(String applicationId,
                                  String inputTopic,
                                  String outputTopic,
                                  String keySchemaFilepath,
                                  String valueSchemaFilepath,
                                  Properties kafkaStreamsProperties)
            throws IOException, URISyntaxException {
        super(applicationId, new String[] {inputTopic}, new String[] {outputTopic}, new String[] {keySchemaFilepath},
                new String[] {valueSchemaFilepath}, kafkaStreamsProperties);
    }

    @Override
    public StreamsBuilder streamsBuilder() {
        DeadLetterHandler deadLetterHandler = (DeadLetterHandler) kafkaStreamsProperties.get(DEAD_LETTER_HANDLER);
        TransformerSupplier<GenericData.Record, GenericData.Record, KeyValue<GenericData.Record, GenericData.Record>> transformer =
                () -> new EncryptTransformer(keySchemas[0], valueSchemas[0], "pumpernickel", deadLetterHandler);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericData.Record, GenericData.Record> stream = builder.stream(inputTopics[0]);
        stream.transform(transformer).to(outputTopics[0]);
        return builder;
    }
}