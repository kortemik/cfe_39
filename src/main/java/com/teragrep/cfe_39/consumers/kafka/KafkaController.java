/*
   HDFS Data Ingestion for PTH_06 use CFE-39
   Copyright (C) 2022  Fail-Safe IT Solutions Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.metrics.*;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaController {
    // ReadCoordinator alone won't allow access to the kafka offsets, it must be done in KafkaReader that is used on rlo_09.
    // ReadCoordinator uses the KafkaReader, but it's set as private in rlo_09 and there are no functions for accessing it through ReadCoordinator.
    // The enable.auto.commit=false is set in config and it is fetched by the config.getKafkaConsumerProperties().
    // cfe_30 is already using enable.auto.commit=false, so looking through cfe_30 and rlo_09 ReadCoordinator and KafkaReader functions should get the coding on right track.

    // A consumer may opt to commit offsets by itself (enable.auto.commit=false).
    // Depending on when it chooses to commit offsets, there are delivery semantics available to the consumer.
    // Exactly once:
    // - For Kafka topic to External System workflows, to effectively achieve exactly once, you must use an idempotent consumer.

    // An Idempotent Consumer pattern uses a Kafka consumer that can consume the same message any number of times, but only process it once.
    // To implement the Idempotent Consumer pattern the recommended approach is to add a table to the database to track processed messages.
    // Each message needs to have a unique messageId assigned by the producing service, either within the payload, or as a Kafka message header.
    // When a new message is consumed the table is checked for the existence of this message Id. If present, then the message is a duplicate.
    // The consumer updates its offsets to effectively mark the message as consumed to ensure it is not redelivered, and no further action takes place.
    // If the message Id is not present in the table then a database transaction is started and the message Id is inserted.
    // The message is then processed performing the required business logic. Upon completion the transaction is committed.

    // requirements: SKIPPING IDEMPOTENT IMPLEMENTATION FOR NOW!
    // 1. The KafkaReader-class must be able to pass the offset values alongside the consumed message to the main class that is calling KafkaReader.
    // 2. The main class must handle the consuming of the kafka topics in an idempotent way as stated above.
    // This is achieved by using the HDFS filenames to store the topic_name and offset values of Kafka topics.
    // In other words Kafka consumers will consume topics normally according to the offsets that Kafka stores internally,
    // but the processor of the consumed messages will check the offsets of the messages that are consumed by kafka and
    // processes ONLY those messages that have not already been processed based on the offset values stored in HDFS filenames.

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaController.class);
    private final Config config;
    private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
    private final List<Thread> threads = new ArrayList<>();
    private final Set<String> activeTopics = new HashSet<>();
    private final DurationStatistics durationStatistics = new DurationStatistics();
    private boolean keepRunning;
    private boolean useMockKafkaConsumer;
    private final int numOfConsumers;
    private Map<TopicPartition, Long> hdfsStartOffsets;

    public KafkaController(Config config) {
        keepRunning = true;
        this.config = config;
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.numOfConsumers = config.getNumOfConsumers();
        this.useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );
        if (useMockKafkaConsumer) {
            this.kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(0); // A consumer used only for scanning the available topics to be allocated to consumers running in different threads (thus 0 as input parameter).
        } else {
            this.kafkaConsumer = new KafkaConsumer<>(config.getKafkaConsumerProperties(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        }
    }

    public void run() throws InterruptedException {

        // register duration statistics
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        Map<TopicPartition, Long> topicPartitionStartMap;
        // Generate offsets of the already committed records for Kafka
        try (HDFSRead hr = new HDFSRead(config) ) {
            topicPartitionStartMap = hr.hdfsStartOffsets();
            LOGGER.info("TESTING topicPartitionStartMap");
            LOGGER.info(topicPartitionStartMap.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        while (keepRunning) {
            LOGGER.debug("Scanning for threads");
            topicScan(durationStatistics, topicCounters);

            // log stuff
            durationStatistics.log();
            long topicScanDelay = 30000L;
            Thread.sleep(topicScanDelay);
            for (String topic_name : activeTopics) {
                LOGGER.info("topic that is being bruned: " + topic_name);
                if (topic_name != null) {
                    try {
                        HDFSPrune hdfsPrune = new HDFSPrune(config, topic_name);
                        hdfsPrune.prune();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            // For testing purposes only. Stops the run when all the records are consumed from the mockConsumer during test.
            if (durationStatistics.getTotalRecords() > 0 & useMockKafkaConsumer) {
                LOGGER.debug("Processed all the test records. Closing.");
                keepRunning = false;
            }

        }
    }

    // Creates kafka topic consumer based on input parameters.
    private void createReader(String topic, List<PartitionInfo> listPartitionInfo, List<TopicCounter> topicCounters) throws SQLException {

        // Create a new topicCounter object for the topic that has not been added to topicCounters-list yet.
        TopicCounter topicCounter = new TopicCounter(topic);
        // Add the new topicCounter object to the list.
        topicCounters.add(topicCounter);

        // Every consumer is run in a separate thread.
        // Consumer group is also handled here, and each consumer of the group runs on separate thread.
        int numOfThreads = Math.min(numOfConsumers, listPartitionInfo.size()); // Makes sure that there aren't more consumers than available partitions in the consumer group.
        for (int testi = 1; numOfThreads >= testi; testi++) {
            // DatabaseOutput handles transferring the consumed data to storage (S3, mariadb, HDFS, etc.)
            // Kafka offset tracking must be included here.
            // Topic is figured out in topicScan so the offsets for the topic should be figured out here.
            Consumer<List<RecordOffset>> output = new DatabaseOutput(
                    config, // Configuration settings
                    topic, // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    topicCounter // TopicCounter object from metrics
            );
            // The kafka offsets must be passed to HDFS. The consumer must also be set to manual commits so the HDFS can handle managing the commit offsets within the HDFS filenames.
            // plain rlo_09.ReadCoordinator won't give access to offset values. Implementing custom rlo_09 code in the package to achieve offset access.
            ReadCoordinator readCoordinator = new ReadCoordinator(
                    topic,
                    config.getKafkaConsumerProperties(),
                    output
            );
            Thread readThread = new Thread(null, readCoordinator, topic+testi); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            threads.add(readThread);
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.
        }

    }

    private void topicScan(DurationStatistics durationStatistics, List<TopicCounter> topicCounters) {
        Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics(Duration.ofSeconds(60)); // Topics can be fetched from mock consumer if the consumer has been updated separately with the partition info.
        Pattern topicsRegex = Pattern.compile(config.getQueueTopicPattern()); // Mock consumer has the partitions in this format: queueTopicPattern=^testConsumerTopic-*$
        // Find the topics available in Kafka based on given QueueTopicPattern, both active and in-active.
        // Need to allow using consumer groups for partition read assignments. aka. load balancing
        Set<String> foundTopics = new HashSet<>();

        // 1. Add functionality so the partition information is also fetched for the queried topics. At the moment only the topic names are fetched.
        Map<String, List<PartitionInfo>> foundPartitions = new HashMap<>();


        for (Map.Entry<String, List<PartitionInfo>> entry : listTopics.entrySet()) {
            Matcher matcher = topicsRegex.matcher(entry.getKey());
            if (matcher.matches()) {
                foundTopics.add(entry.getKey());

                // 2. Add functionality so the partition information is also fetched for the queried topics.
                foundPartitions.put(entry.getKey(), entry.getValue());

            }
        }

        if (foundTopics.isEmpty()) {
            throw new IllegalStateException("Pattern <[" + config.getQueueTopicPattern() + "]> found no topics." );
        }

        // subtract currently active topics from found topics
        foundTopics.removeAll(activeTopics);
        // 3. Subtract currently active partitions from found partitions
        for (String topic_name : activeTopics) {
            foundPartitions.remove(topic_name); // removes the partitions from the list based on the topic name.
        }


        // Activate all the found in-active topics, in other words create consumer groups for all of them using the createReader()-function.
        foundPartitions.forEach((k, v) -> {
            LOGGER.debug("Activating topic <"+k+">");
            try {
                createReader(k, v, topicCounters);
                activeTopics.add(k);
                durationStatistics.addAndGetThreads(1);
            } catch (SQLException sqlException) {
                LOGGER.error("Topic <"+k+"> not activated due to reader creation error: " + sqlException);
            }
        });
        durationStatistics.report();
    }

}
