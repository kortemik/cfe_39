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

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * <h1>Mock Kafka Consumer Factory</h1> Mocked Kafka Consumer factory used for testing.
 *
 * @since 08/06/2022
 * @author Mikko Kortelainen
 */
@VisibleForTesting
public class MockKafkaConsumerFactory {

    final static private Logger LOGGER = LoggerFactory.getLogger(MockKafkaConsumerFactory.class);

    private MockKafkaConsumerFactory() {
    }

    private static void generateEvents(MockConsumer<byte[], byte[]> consumer, String topicName, int partition) {
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                0L,
                                "2022-04-25T07:34:50.804Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:50.804Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"835bf792-91cf-44e3-976b-518330bb8fd3\" source=\"source\" unixtime=\"1650872090805\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                1L,
                                "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                2L,
                                "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"1848d8a1-2f08-4a1e-bec4-ff9e6dd92553\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi."
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                3L,
                                "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"5e1a0398-c2a0-468d-a562-c3bb31f0f853\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi."
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                4L,
                                "2022-04-25T07:34:50.822Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:50.822Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02\"][event_id@48577 hostname=\"jla-02\" uuid=\"6268c3a2-5bda-427f-acce-29416eb445f4\" source=\"source\" unixtime=\"1650872090822\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] 470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi."
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                5L,
                                "2022-04-25T07:34:52.238Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.238Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"b500dcaf-1101-4000-b6b9-bfb052ddbf86\" source=\"source\" unixtime=\"1650872092238\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                6L,
                                "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"05363122-51ac-4c0b-a681-f5868081f56d\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );

        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                7L,
                                "2022-04-25T07:34:52.239Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.239Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"7bbcd843-b795-4c14-b4a1-95f5d445cbcd\" source=\"source\" unixtime=\"1650872092239\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                8L,
                                "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"2bc0a9f9-237d-4656-b40a-3038aace37f0\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                9L,
                                "2022-04-25T07:34:52.240Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.240Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ecf61e8d-e3a7-48ef-9b73-3c5a5243d2e6\" source=\"source\" unixtime=\"1650872092240\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                10L,
                                "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"bf101d5a-e816-4f51-b132-97f8e3431f8e\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                11L,
                                "2022-04-25T07:34:52.241Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.241Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"ef94d9e9-3c44-4892-b5a6-bf361d13ff97\" source=\"source\" unixtime=\"1650872092241\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                12L,
                                "2022-04-25T07:34:52.242Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.242Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"5bce6e3d-767d-44b4-a044-6c4872f8f2b5\" source=\"source\" unixtime=\"1650872092242\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
        consumer
                .addRecord(
                        new ConsumerRecord<>(
                                topicName,
                                partition,
                                13L,
                                "2022-04-25T07:34:52.243Z".getBytes(StandardCharsets.UTF_8),
                                "<12>1 2022-04-25T07:34:52.243Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"3bb55ce4-0ea7-413a-b403-28b174d7ac99\" source=\"source\" unixtime=\"1650872092243\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872092\"] 25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                                        .getBytes(StandardCharsets.UTF_8)
                        )
                );
    }

    // Can initialize topic scan with all partitions available when the input parameter is 0. Consumer is manually assigned to specific partitions depending on the threadnum parameter. For example on threadnum 1 consumer has odd numbered partitions assigned to it and threadnum 2 has the even numbered partitions.
    public static Consumer<byte[], byte[]> getConsumer(int threadnum) {

        LOGGER.warn("useMockKafkaConsumer is set, using MockKafkaConsumer");
        int amountofloops = 10; // number of loops for adding partitions/records to the mock consumer topic. Each loop adds a new partition of 14 records. 17777 loops results in file size slightly above 64M. 10 loops is sized at 36,102 bits.
        final MockConsumer<byte[], byte[]> consumer;
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        List<TopicPartition> topicPartitions = new ArrayList<>();
        LinkedHashMap<TopicPartition, Long> beginningOffsets = new LinkedHashMap<>();
        LinkedHashMap<TopicPartition, Long> endOffsets = new LinkedHashMap<>();
        List<PartitionInfo> mockPartitionInfo = new ArrayList<>();
        // generate the topic partitions and metadata first
        for (int i = 0; i < amountofloops; i++) {
            TopicPartition topicPartition = new TopicPartition("testConsumerTopic", i);
            topicPartitions.add(topicPartition);
            beginningOffsets.put(topicPartition, 0L);
            endOffsets.put(topicPartition, 14L);
            mockPartitionInfo.add(new PartitionInfo("testConsumerTopic", i, null, null, null));
        }

        if (threadnum == 1) {
            List<TopicPartition> oddTopicPartitions = new ArrayList<>();
            for (TopicPartition a : topicPartitions) {
                if (((a.partition() + 1) % 2) == 0) {
                    oddTopicPartitions.add(a);
                }
            }
            consumer.assign(oddTopicPartitions); // assign
            for (TopicPartition a : topicPartitions) {
                if (((a.partition() + 1) % 2) == 0) {
                    generateEvents(consumer, a.topic(), a.partition());
                }
            }
        }
        else if (threadnum == 2) {
            List<TopicPartition> evenTopicPartitions = new ArrayList<>();
            for (TopicPartition a : topicPartitions) {
                if (((a.partition() + 1) % 2) != 0) {
                    evenTopicPartitions.add(a);
                }
            }
            consumer.assign(evenTopicPartitions); // assign
            for (TopicPartition a : topicPartitions) {
                if (((a.partition() + 1) % 2) != 0) {
                    generateEvents(consumer, a.topic(), a.partition());
                }
            }
        }
        else {
            consumer.assign(topicPartitions); // assign
            for (TopicPartition a : topicPartitions) {
                generateEvents(consumer, a.topic(), a.partition());
            }
        }

        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.updateEndOffsets(endOffsets);
        consumer.updatePartitions("testConsumerTopic", mockPartitionInfo);
        return consumer;
    }
}
