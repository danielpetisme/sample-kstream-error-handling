package com.examples.danielpetisme;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class Topics {

    private final Logger logger = LoggerFactory.getLogger(Topics.class);

    public final String inputTopic;
    public final String outputTopic;

    public Topics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        inputTopic = System.getenv().getOrDefault("INPUT_TOPIC", "input");
        outputTopic = System.getenv().getOrDefault("OUTPUT_EVENT_TOPIC", "output");

        final Integer numberOfPartitions = Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS", "1"));
        final Short replicationFactor = Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR", "1"));


        createTopic(adminClient, inputTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, outputTopic, numberOfPartitions, replicationFactor);
    }

    public void createTopic(AdminClient adminClient, String topicName, Integer numberOfPartitions, Short replicationFactor) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);
            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }
}
