/*
 * HDFS Data Ingestion for PTH_06 use CFE-39
 * Copyright (C) 2021-2024 Suomen Kanuuna Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 *
 * Additional permission under GNU Affero General Public License version 3
 * section 7
 *
 * If you modify this Program, or any covered work, by linking or combining it
 * with other code, such other code is not for that reason alone subject to any
 * of the requirements of the GNU Affero GPL version 3 as long as this Program
 * is the same Program as licensed from Suomen Kanuuna Oy without any additional
 * modifications.
 *
 * Supplemented terms under GNU Affero General Public License version 3
 * section 7
 *
 * Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
 * versions must be marked as "Modified version of" The Program.
 *
 * Names of the licensors and authors may not be used for publicity purposes.
 *
 * No rights are granted for use of trade names, trademarks, or service marks
 * which are in The Program if any.
 *
 * Licensee must indemnify licensors and authors for any liability that these
 * contractual assumptions impose on licensors and authors.
 *
 * To the extent this program is licensed as part of the Commercial versions of
 * Teragrep, the applicable Commercial License may apply to this file if you as
 * a licensee so wish it.
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

// Ingests data for HDFS database, periodically scans kafka for new topics based on config.getQueueTopicPattern() and creates kafka topic consumer groups for the new topics that will store the records to HDFS.
public class HdfsDataIngestion {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsDataIngestion.class);
    private final Config config;
    private final org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
    private final List<Thread> threads = new ArrayList<>();
    private final Set<String> activeTopics = new HashSet<>();
    private boolean keepRunning;
    private boolean useMockKafkaConsumer;
    private final int numOfConsumers;
    private Map<TopicPartition, Long> hdfsStartOffsets;

    public HdfsDataIngestion(Config config) {
        keepRunning = true;
        this.config = config;
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.numOfConsumers = config.getNumOfConsumers();
        this.useMockKafkaConsumer = Boolean
                .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
        if (useMockKafkaConsumer) {
            this.kafkaConsumer = MockKafkaConsumerFactory.getConsumer(0); // A consumer used only for scanning the available topics to be allocated to consumers running in different threads (thus 0 as input parameter).
        }
        else {
            this.kafkaConsumer = new KafkaConsumer<>(
                    config.getKafkaConsumerProperties(),
                    new ByteArrayDeserializer(),
                    new ByteArrayDeserializer()
            );
        }
        hdfsStartOffsets = new HashMap<>();
    }

    public void run() throws InterruptedException {

        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        // Generates offsets of the already committed records for Kafka and passes them to the kafka consumers.
        try (HDFSRead hr = new HDFSRead(config)) {
            hdfsStartOffsets = hr.hdfsStartOffsets();
            LOGGER.debug("topicPartitionStartMap generated succesfully: <{}>", hdfsStartOffsets);
        }
        catch (IOException e) {
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
                LOGGER.info("topic that is being pruned: <{}>", topic_name);
                if (topic_name != null) {
                    try {
                        HDFSPrune hdfsPrune = new HDFSPrune(config, topic_name);
                        hdfsPrune.prune();
                    }
                    catch (IOException e) {
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
    private void createReader(
            String topic,
            List<PartitionInfo> listPartitionInfo,
            List<TopicCounter> topicCounters,
            DurationStatistics durationStatistics
    ) throws SQLException {

        // Create a new topicCounter object for the topic that has not been added to topicCounters-list yet.
        TopicCounter topicCounter = new TopicCounter(topic);
        // Add the new topicCounter object to the list.
        topicCounters.add(topicCounter);

        /* Every consumer is run in a separate thread.
         Consumer group is also handled here, and each consumer of the group runs on separate thread.*/
        int numOfThreads = Math.min(numOfConsumers, listPartitionInfo.size()); // Makes sure that there aren't more consumers than available partitions in the consumer group.
        for (int testi = 1; numOfThreads >= testi; testi++) {
            Consumer<List<RecordOffset>> output = new DatabaseOutput(
                    config, // Configuration settings
                    topic, // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    topicCounter // TopicCounter object from metrics
            );
            ReadCoordinator readCoordinator = new ReadCoordinator(
                    topic,
                    config.getKafkaConsumerProperties(),
                    output,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, topic + testi); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            threads.add(readThread);
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.
        }

    }

    private void topicScan(DurationStatistics durationStatistics, List<TopicCounter> topicCounters) {
        Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics(Duration.ofSeconds(60));
        Pattern topicsRegex = Pattern.compile(config.getQueueTopicPattern());
        //         Find the topics available in Kafka based on given QueueTopicPattern, both active and in-active.
        Set<String> foundTopics = new HashSet<>();
        Map<String, List<PartitionInfo>> foundPartitions = new HashMap<>();
        for (Map.Entry<String, List<PartitionInfo>> entry : listTopics.entrySet()) {
            Matcher matcher = topicsRegex.matcher(entry.getKey());
            if (matcher.matches()) {
                foundTopics.add(entry.getKey());
                foundPartitions.put(entry.getKey(), entry.getValue());
            }
        }
        if (foundTopics.isEmpty()) {
            throw new IllegalStateException("Pattern <[" + config.getQueueTopicPattern() + "]> found no topics.");
        }
        // subtract currently active topics from found topics
        foundTopics.removeAll(activeTopics);
        // Subtract currently active partitions from found partitions
        for (String topic_name : activeTopics) {
            foundPartitions.remove(topic_name); // removes the partitions from the list based on the topic name.
        }

        // Activate all the found in-active topics, in other words create consumer groups for all of them using the createReader()-function.
        foundPartitions.forEach((k, v) -> {
            LOGGER.debug("Activating topic <{}>", k);
            try {
                createReader(k, v, topicCounters, durationStatistics);
                activeTopics.add(k);
                durationStatistics.addAndGetThreads(1);
            }
            catch (SQLException sqlException) {
                LOGGER.error("Topic <{}> not activated due to reader creation error: ", k, sqlException);
            }
        });
        durationStatistics.report();
    }

}
