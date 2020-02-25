package com.opi.kafka.streams.generic;

import com.opi.kafka.streams.SchemaLoader;
import com.opi.kafka.streams.StreamStateListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

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
public class GenericStream {

    protected final String applicationId;
    protected final String[] inputTopics;
    protected final String[] outputTopics;
    protected final Schema[] keySchemas;
    protected final Schema[] valueSchemas;
    protected final Properties kafkaStreamsProperties;

    private KafkaStreams streams;
    private CountDownLatch closeLatch = new CountDownLatch(1);
    private ReentrantLock listenerLock = new ReentrantLock();
    private List<StreamStateListener> listeners = new ArrayList<>();

    public GenericStream(String applicationId,
                         String inputTopic,
                         String outputTopic,
                         String keySchemaFilepath,
                         String valueSchemaFilepath,
                         Properties kafkaStreamsProperties)
            throws IOException, URISyntaxException {
        this(applicationId, new String[] {inputTopic}, new String[] {outputTopic}, new String[] {keySchemaFilepath},
                new String[] {valueSchemaFilepath}, kafkaStreamsProperties);
    }

    public GenericStream(String applicationId,
                         String[] inputTopics,
                         String[] outputTopics,
                         String[] keySchemaFilepaths,
                         String[] valueSchemaFilepaths,
                         Properties kafkaStreamsProperties)
            throws IOException, URISyntaxException {

        this.applicationId = applicationId;
        this.inputTopics = inputTopics;
        this.outputTopics = outputTopics;
        this.kafkaStreamsProperties = kafkaStreamsProperties;

        SchemaLoader schemaLoader = new SchemaLoader();

        this.keySchemas = new Schema[keySchemaFilepaths.length];
        for (int i = 0; i < keySchemaFilepaths.length; i++) {
            this.keySchemas[i] = schemaLoader.getSchema(keySchemaFilepaths[i]);
        }

        this.valueSchemas = new Schema[valueSchemaFilepaths.length];
        for (int i = 0; i < valueSchemaFilepaths.length; i++) {
            this.valueSchemas[i] = schemaLoader.getSchema(valueSchemaFilepaths[i]);
        }
        setup();
    }

    public void start() {

        // start the stream and wait until it is told to close by an outside thread calling its close() method or an
        // exception occurs
        try {
            streams.start();
            closeLatch.await();
        } catch (Throwable t) {
            log.error(format("Fatal error occurred in stream \"%s\"", applicationId), t);
        }
        // close stream if exception occurred or close() was called
        streams.close();
        log.info(format("\"%s\" stream closed", applicationId));

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

    public StreamsBuilder streamsBuilder() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<GenericData.Record, GenericData.Record> stream = builder.stream(inputTopics[0]);
        stream.to(outputTopics[0]);
        return builder;
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

        kafkaStreamsProperties.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE);
        final Topology topology = streamsBuilder().build(kafkaStreamsProperties);
        final TopologyDescription description = topology.describe();
        log.info("Topology: {}", description);

        // application.id is used as Kafka consumer group.id for coordination in kstreams, so you cannot set group.id explicitly
        kafkaStreamsProperties.put("application.id", format("spring-%s-stream", applicationId));
        this.streams = new KafkaStreams(topology, kafkaStreamsProperties);
        //this.streams.setUncaughtExceptionHandler((thread, throwable) -> log.error(format("Error occurred in stream \"%s\"", applicationId), throwable));

        // add a state listener to detect if an error occurs while streaming and close this stream immediately - fail
        // fast strategy
        this.streams.setStateListener((newState, oldState) -> {
            if (newState.equals(KafkaStreams.State.ERROR)) {
                log.error(format("Fatal error occurred and now closing stream \"%s\"", applicationId));
            }
        });
    }
}