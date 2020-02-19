package com.opi.kafka.streams.generic;

import com.opi.kafka.streams.SchemaLoader;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.lang.String.format;

public class GenericDataRecordStream {

    private String applicationId;
    private String inputTopic;
    private String outputTopic;
    private Schema keySchema;
    private Schema valueSchema;
    private Properties kafkaStreamsProperties;
    private KafkaStreams streams;
    private CountDownLatch latch = new CountDownLatch(1);

    @Autowired
    public GenericDataRecordStream(String applicationId,
                                   String inputTopic,
                                   String outputTopic,
                                   String keySchemaFilepath,
                                   String valueSchemaFilepath,
                                   Properties kafkaStreamsProperties)
            throws IOException, URISyntaxException {

        this.applicationId = applicationId;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.kafkaStreamsProperties = kafkaStreamsProperties;
        SchemaLoader schemaLoader = new SchemaLoader();
        this.keySchema = schemaLoader.getSchema(keySchemaFilepath);
        this.valueSchema = schemaLoader.getSchema(valueSchemaFilepath);
        setup();
    }

    public int start() {
        try {
            streams.start();
            latch.await();
        } catch (Throwable t) {
            return 1;
        }
        streams.close();
        return 0;
    }

    public void close() {
        latch.countDown();
    }

    private void setup() {

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericData.Record, GenericData.Record> stream = builder.stream(inputTopic);
        stream.transform(() -> new EncryptTransformer("pumpernickel", keySchema, valueSchema)).to(outputTopic);

        final Topology topology = builder.build();
        // application.id is used as Kafka consumer group.id for coordination in kstreams, so you cannot set group.id explicitly
        kafkaStreamsProperties.put("application.id", format("spring-%s-stream", applicationId));
        this.streams = new KafkaStreams(topology, kafkaStreamsProperties);
    }
}