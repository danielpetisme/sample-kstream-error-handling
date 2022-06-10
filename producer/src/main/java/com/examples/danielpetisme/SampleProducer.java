package com.examples.danielpetisme;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SampleProducer {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SampleProducer.class);
    private final Topics topics;
    private final Properties properties;
    private final Long messageBackOff;
    PrometheusMeterRegistry prometheusRegistry;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new SampleProducer(Collections.emptyMap()).start();
    }

    public SampleProducer(Map<String, String> config) throws InterruptedException, ExecutionException {
        this.properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);

        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);

        messageBackOff = Long.valueOf(System.getenv().getOrDefault("MESSAGE_BACKOFF_MS", "1000"));

        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        int port = Integer.parseInt(System.getenv().getOrDefault("PROMETHEUS_PORT", "8080"));
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
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

    private void start() throws InterruptedException {
        logger.info("creating producer with props: {}", properties);


        logger.info("Sending data to `{}` topic", topics.inputTopic);
        try (Producer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer())) {
            new KafkaClientMetrics(producer).bindTo(prometheusRegistry);

            long id = 0;
            while (true) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topics.inputTopic, String.valueOf(id), String.valueOf(id));
                logger.info("Sending Key = {}, Value = {}", record.key(), record.value());
                producer.send(record, (recordMetadata, exception) -> sendCallback(record, recordMetadata, exception));
                id++;
                TimeUnit.MILLISECONDS.sleep(messageBackOff);
            }
        }
    }

    private void sendCallback(ProducerRecord<String, String> record, RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            logger.debug("succeeded sending. offset: {}", recordMetadata.offset());
        } else {
            logger.error("failed sending key: {}" + record.key(), e);
        }
    }


    private Map<String, String> defaultProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49881"
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
