package com.opi.kafka.streams.generic;

import com.opi.kafka.streams.SchemaLoader;
import com.opi.kafka.streams.StreamStateListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import static java.lang.String.format;

@Slf4j
public class GenericDataRecordStream {

    private String applicationId;
    private String inputTopic;
    private String outputTopic;
    private Schema keySchema;
    private Schema valueSchema;
    private Properties kafkaStreamsProperties;
    private KafkaStreams streams;
    private CountDownLatch closeLatch = new CountDownLatch(1);
    private ReentrantLock listenerLock = new ReentrantLock();
    private List<StreamStateListener> listeners = new ArrayList<>();

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

    public void start() {

        // start the stream and wait until it is told to close by an outside thread calling its close() method or an
        // exception occurs
        try {
            streams.start();
            closeLatch.await();
        } catch (Throwable t) {
            log.error("Fatal error occurred in stream", t);
        }
        // close stream if exception occurred or close() was called
        streams.close();

        // notify stream listeners that the stream has closed
        notifyStateChange(StreamStateListener::closed);
    }

    public void close() {
        closeLatch.countDown();
    }

    public void addListener(StreamStateListener listener) {
        listenerLock.lock();
        try {
            this.listeners.add(listener);
        } finally {
            listenerLock.unlock();
        }
    }

    private void notifyStateChange(Consumer<? super StreamStateListener> action) {
        listenerLock.lock();
        try {
            this.listeners.forEach(action);
        } finally {
            listenerLock.unlock();
        }
    }

    private void setup() {

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericData.Record, GenericData.Record> stream = builder.stream(inputTopic);
        stream.transform(() -> new EncryptTransformer("pumpernickel", keySchema, valueSchema)).to(outputTopic);

        final Topology topology = builder.build();
        // application.id is used as Kafka consumer group.id for coordination in kstreams, so you cannot set group.id explicitly
        kafkaStreamsProperties.put("application.id", format("spring-%s-stream", applicationId));
        this.streams = new KafkaStreams(topology, kafkaStreamsProperties);

        // add a state listener to detect if an error occurs while streaming and close this stream immediately - fail
        // fast strategy
        this.streams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error("Fatal error occurred and now closing stream");
                close();
            }
        });
    }
}