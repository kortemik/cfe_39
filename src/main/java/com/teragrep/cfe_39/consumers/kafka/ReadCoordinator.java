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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class ReadCoordinator implements Runnable {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReadCoordinator.class);

    private final String queueTopic;
    private final Properties readerKafkaProperties;
    private final Consumer<List<RecordOffset>> callbackFunction;
    private boolean run = true;
    private final Map<TopicPartition, Long> hdfsStartOffsets;

    public ReadCoordinator(
            String queueTopic,
            Properties readerKafkaProperties,
            Consumer<List<RecordOffset>> callbackFunction,
            Map<TopicPartition, Long> hdfsStartOffsets)
    {
        this.queueTopic = queueTopic;
        this.readerKafkaProperties = readerKafkaProperties;
        this.callbackFunction = callbackFunction;
        this.hdfsStartOffsets = hdfsStartOffsets;
    }

    private KafkaReader createKafkaReader(Properties readerKafkaProperties,
                                          String topic,
                                          Consumer<List<RecordOffset>> callbackFunction,
                                          boolean useMockKafkaConsumer) {

        org.apache.kafka.clients.consumer.Consumer<byte[], byte[]> kafkaConsumer;
        if (useMockKafkaConsumer) { // Mock kafka consumer is enabled, create mock consumers with assigned partitions that are not overlapping with each other.
            String name = Thread.currentThread().getName(); // Use thread name to identify which thread is running the code.
            if (Objects.equals(name, "testConsumerTopic1")) {
                kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(1); // creates a Kafka MockConsumer that has the odd numbered partitions assigned to it.
            }else {
                kafkaConsumer = MockKafkaConsumerFactoryTemp.getConsumer(2); // creates a Kafka MockConsumer that has the even numbered partitions assigned to it.
            }
        } else { // Mock kafka consumer is disabled, subscribe method should handle assigning the partitions automatically to the consumer based on group id parameters of readerKafkaProperties.
            kafkaConsumer = new KafkaConsumer<>(readerKafkaProperties, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            kafkaConsumer.subscribe(Collections.singletonList(topic));
        }

        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        // Seek the consumer to topic partition offset defined by the latest record that is committed to HDFS.
        for (TopicPartition topicPartition : assignment) {
            if (hdfsStartOffsets.containsKey(topicPartition)) {
                long position = kafkaConsumer.position(topicPartition);
                if (position < hdfsStartOffsets.get(topicPartition)) {
                    kafkaConsumer.seek(topicPartition, hdfsStartOffsets.get(topicPartition));
                }
            }
        }

        return new KafkaReader(kafkaConsumer, callbackFunction);
    }

    // Part or Runnable implementation, called when the thread is started.
    @Override
    public void run() {
        boolean useMockKafkaConsumer = Boolean.parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
        try (
                KafkaReader kafkaReader = createKafkaReader(
                        readerKafkaProperties,
                        queueTopic,
                        callbackFunction,
                        useMockKafkaConsumer
                )
        ) {
            while (run) {
                kafkaReader.read();
            }
        }
    }

    // remove?
    public void stop() {
        run = false;
    }
}