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
package com.teragrep.cfe_39;

import com.teragrep.cfe_39.consumers.kafka.ReadCoordinator;
import com.teragrep.cfe_39.consumers.kafka.RecordOffset;
import com.teragrep.rlo_06.RFC5424Frame;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KafkaConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);

    @Test
    public void readCoordinatorTest2Threads() {
        assertDoesNotThrow(() -> {
            Config config = new Config();
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            ArrayList<List<RecordOffset>> messages = new ArrayList<>();
            Consumer<List<RecordOffset>> output = message -> messages.add(message);

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    output,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic1"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(1000);

            ReadCoordinator readCoordinator2 = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    output,
                    hdfsStartOffsets
            );
            Thread readThread2 = new Thread(null, readCoordinator2, "testConsumerTopic2"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread2.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);
            Assertions.assertEquals(2, messages.size());
            Assertions.assertEquals(140, messages.get(0).size() + messages.get(1).size()); // Assert that expected amount of records has been consumed by the consumer group.
            Assertions.assertEquals(70, messages.get(0).size());
            Assertions.assertEquals(70, messages.get(1).size());

            // Assert that all the record contents are correct, every topic partition has identical set of offset-message pairings.
            List<String> messageList = new ArrayList<String>();
            messageList.add("[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!");
            messageList.add("[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!");
            messageList.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.");
            messageList.add("470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.");
            messageList.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.");
            messageList
                    .add(
                            "25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                    );
            messageList
                    .add(
                            "25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                    );

            RFC5424Frame rfc5424Frame = new RFC5424Frame(false);

            RecordOffset recordOffset;

            Iterator<String> iterator = messageList.iterator();
            int counter = 0;
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"7\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"5\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"3\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"1\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"9\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            Assertions.assertEquals(70, counter);

            counter = 0;
            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"8\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"6\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"4\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"2\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = messageList.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(1).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"0\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }
            Assertions.assertEquals(70, counter);

        });
    }

    @Test
    public void readCoordinatorTest1Thread() {
        assertDoesNotThrow(() -> {
            Config config = new Config();
            Map<TopicPartition, Long> hdfsStartOffsets = new HashMap<>();
            ArrayList<List<RecordOffset>> messages = new ArrayList<>();
            Consumer<List<RecordOffset>> output = message -> messages.add(message);

            ReadCoordinator readCoordinator = new ReadCoordinator(
                    "testConsumerTopic",
                    config.getKafkaConsumerProperties(),
                    output,
                    hdfsStartOffsets
            );
            Thread readThread = new Thread(null, readCoordinator, "testConsumerTopic0"); // Starts the thread with readCoordinator that creates the consumer and subscribes to the topic.
            readThread.start(); // Starts the thread, in other words proceeds to call run() function of ReadCoordinator.

            Thread.sleep(10000);
            Assertions.assertEquals(1, messages.size());
            Assertions.assertEquals(140, messages.get(0).size()); // Assert that expected amount of records has been consumed by the consumer.

            // Assert that all the record contents are correct, every topic partition has identical set of offset-message pairings.
            List<String> list = new ArrayList<String>();
            list.add("[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!");
            list.add("[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!");
            list.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.");
            list.add("470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.");
            list.add("470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.");
            list
                    .add(
                            "25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]"
                    );
            list
                    .add(
                            "25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]"
                    );

            RFC5424Frame rfc5424Frame = new RFC5424Frame(false);

            RecordOffset recordOffset;

            Iterator<String> iterator = list.iterator();
            int counter = 0;
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"7\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"8\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertTrue(iterator.hasNext());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"5\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"6\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"3\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"4\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"1\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"2\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"0\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            iterator = list.iterator();
            for (int i = 0; i <= 13; i++) {
                recordOffset = messages.get(0).get(counter);
                Assertions
                        .assertEquals(
                                "{\"topic\":\"testConsumerTopic\", \"partition\":\"9\", \"offset\":\"" + i + "\"}",
                                recordOffset.offsetToJSON()
                        );
                rfc5424Frame.load(new ByteArrayInputStream(recordOffset.getRecord()));
                Assertions.assertTrue(rfc5424Frame.next());
                Assertions.assertEquals(iterator.next(), rfc5424Frame.msg.toString());
                Assertions.assertFalse(rfc5424Frame.next());
                counter++;
            }

            Assertions.assertEquals(140, counter); // All 140 records asserted.

        });
    }

}
