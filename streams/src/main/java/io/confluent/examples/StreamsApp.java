package io.confluent.examples;

import com.examples.model.ValueContainer;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class StreamsApp {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final static Logger logger = LoggerFactory.getLogger(StreamsApp.class);
    private final KafkaStreams streams;
    private final Topics topics;
    PrometheusMeterRegistry prometheusRegistry;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new StreamsApp(Collections.emptyMap()).start();
    }

    public StreamsApp(Map<String, String> config) throws InterruptedException, ExecutionException {
        var properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        var topology = buildTopology();
        logger.info(topology.describe().toString());
        logger.info("Creating streams with props: {}", properties);
        streams = new KafkaStreams(topology, properties);
        streams.setStateListener((before, after) -> logger.warn("Switching from state {} to {}", before, after));
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));

        streams.setUncaughtExceptionHandler(exception -> {
            logger.error("Something bad happen: {}", exception.getMessage());
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });


        new KafkaStreamsMetrics(this.streams).bindTo(prometheusRegistry);

        int port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080"));
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            server.createContext("/topology", httpExchange -> {
                String response = topology.describe().toString();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            server.createContext("/metadata", httpExchange -> {
                Set<ThreadMetadata> metadata = streams.metadataForLocalThreads();
                StringBuilder builder = new StringBuilder();
                for (ThreadMetadata thread : metadata) {
                    builder.append("Thread: " + thread.threadName()).append("\n");
                    for (TaskMetadata task : thread.activeTasks()) {
                        builder.append("  Task: " + task.taskId()).append("\n");
                        for (TopicPartition topicPartition : task.topicPartitions())
                            builder.append("    Topic: " + topicPartition.topic()).append(", Partition:" + topicPartition.partition()).append("\n");
                    }
                }
                String response = builder.toString();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    Topology buildTopology() {

        var builder = new StreamsBuilder();
        var input = builder.stream(
                topics.inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));
        input
                .mapValues((String it) -> {
                    ValueContainer<String> valueContainer = new ValueContainer<>();
                    try {
                        if(it.startsWith("abc")) {
                            throw new IllegalArgumentException("abc is not accepted");
                        }
                        valueContainer.setValue(it.toUpperCase());
                    } catch (Exception e) {
                        valueContainer.setException(e.getMessage());
                    }
                    return valueContainer;
                })
                .split()
                .branch((k, v) -> v.getException() != null, Branched.withConsumer(ks -> ks.mapValues(it -> it.getException()).to(topics.dlqTopic)))
                .defaultBranch(Branched.withConsumer(ks -> ks.mapValues(it -> it.getValue()).to(topics.outputTopic)));

        return builder.build();

    }

    public void start() {
        logger.info("Kafka Streams started");
        streams.start();
    }

    public void stop() {
        streams.close(Duration.ofSeconds(3));
        logger.info("Kafka Streams stopped");
    }

    private Map<String, String> defaultProps = Map.of(
//            StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.TRACE.name,
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", StreamsApp.class.getName()),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "1",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
    );

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix, Map<String, String> customProps) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        props.putAll(customProps);
        return props;
    }
}
