package io.confluent.examples;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

public class StreamsAppTest {

    KafkaProducer<String, String> inputProducer;
    KafkaConsumer<String, String> inputConsumer;
    KafkaConsumer<String, String> outputConsumer;
    KafkaConsumer<String, String> dlqConsumer;
    Topics topics;
    StreamsApp streamsApp;

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"));

    @Before
    public void beforeEach() throws ExecutionException, InterruptedException {
        topics = new Topics(AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        )));

        inputProducer = new KafkaProducer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, "sample-producer",
                        ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE,
                        ProducerConfig.ACKS_CONFIG, "all",
                        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
                ),
                new StringSerializer(), new StringSerializer()
        );

        inputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "inputConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );
        inputConsumer.subscribe(Collections.singletonList(topics.inputTopic));

        outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "outputConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );

        outputConsumer.subscribe(Collections.singletonList(topics.outputTopic));

        dlqConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, "dlqConsumer",
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new StringDeserializer()
        );

        dlqConsumer.subscribe(Collections.singletonList(topics.dlqTopic));
    }

    @After
    public void afterEach() {
        streamsApp.stop();
    }

    private void loadInput(List<KeyValue<String, String>> input) {
        input.forEach(it -> {
            System.out.println("Producing key=" + it.key + ", Value= " + it.value);
            try {
                inputProducer.send(
                        new ProducerRecord<>(topics.inputTopic, 0, System.currentTimeMillis(), it.key, it.value),
                        (RecordMetadata metadata, Exception exception) -> {
                            if (exception != null) {
                                fail(exception.getMessage());
                            }
                            System.out.println("Produced " + metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                        }
                ).get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        List<KeyValue<String, String>> loaded = new ArrayList<>();

        long start = System.currentTimeMillis();
        while (loaded.isEmpty() && System.currentTimeMillis() - start < 20_000) {
            ConsumerRecords<String, String> records = inputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
            records.forEach((record) -> loaded.add(new KeyValue<>(record.key(), record.value())));
        }
        assertThat(loaded).hasSize(input.size());
    }

    @Test
    public void testErrorHandling() throws Exception {
        streamsApp = new StreamsApp(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
                )
        );
        streamsApp.start();

        loadInput(List.of(
                new KeyValue<>("1", "abc"),
                new KeyValue<>("2", "def"),
                new KeyValue<>("3", "ghi")
        ));


        //Verify DLQ has 2 messages
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<ConsumerRecord> loaded = new ArrayList<>();
                    ConsumerRecords<String, String> records = outputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                    records.forEach((record) -> {
                        loaded.add(record);
                        System.out.println("Output Topic: " + record.value());
                    });
                    assertThat(loaded).isNotEmpty();
                    assertThat(loaded.size()).isEqualTo(2);
                });

        //Verify DLQ has 1 message
        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<ConsumerRecord> loaded = new ArrayList<>();
                    ConsumerRecords<String, String> records = dlqConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                    records.forEach((record) -> {
                        loaded.add(record);
                        System.out.println("DLQ Topic: " + record.value());
                    });
                    assertThat(loaded).isNotEmpty();
                    assertThat(loaded.size()).isEqualTo(1);
                    assertThat((String) loaded.get(0).value()).isEqualToIgnoringCase("abc is not accepted");
                });
    }

}