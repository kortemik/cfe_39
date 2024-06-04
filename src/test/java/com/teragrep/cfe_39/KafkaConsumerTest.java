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

import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.Assertions;
import com.teragrep.cfe_39.avro.SyslogRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class KafkaConsumerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerTest.class);
    // Make sure application.properties has consumer.useMockKafkaConsumer=true enabled for Kafka testing.

    @Disabled
    @Test
    public void configTest() {
        // Configuration tests done, configurations working correctly with the right .jaas and .properties files.
        assertDoesNotThrow(() -> {
            Config config = new Config();
            Properties readerKafkaProperties = config.getKafkaConsumerProperties();
            // Test extracting useMockKafkaConsumer value from config.
            boolean useMockKafkaConsumer = Boolean
                    .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));
            LOGGER.debug("useMockKafkaConsumer: " + useMockKafkaConsumer);
        });
    }

    @Disabled
    @Test
    public void kafkaAndAvroFullTest() {
        assertDoesNotThrow(() -> {
            Config config = new Config();
            config.setMaximumFileSize(3000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bytes.
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            hdfsDataIngestion.run();
            int counter = avroReader(1, 2);
            Assertions.assertEquals(140, counter);
            cleanup(config, 1, 2);
        });
    }

    // Reads the data from a list of avro files
    public int avroReader(int start, int end) {
        return assertDoesNotThrow(() -> {
            // Deserialize Users from disk
            Config config = new Config();
            Path queueDirectory = Paths.get(config.getQueueDirectory());
            int looper = 0;
            int counter = 0;
            int partitionCounter = 0;
            for (int j = 0; j <= 9; j++) {
                for (int i = start; i <= end; i++) {
                    File syslogFile = new File(
                            queueDirectory.toAbsolutePath() + File.separator + "testConsumerTopic" + j + "." + i
                    );
                    DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
                    try (
                            DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(
                                    syslogFile,
                                    userDatumReader
                            )
                    ) {
                        SyslogRecord user = null;
                        while (dataFileReader.hasNext()) {
                            user = dataFileReader.next(user);
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(syslogFile.getPath());
                                LOGGER.debug(user.toString());
                            }
                            counter++;
                            // All the mock data is generated from a set of 14 records.
                            if (looper <= 0) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872090804000, \"message\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 0, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 1) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872090806000, \"message\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 1, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 2) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 2, \"origin\": \"jla-02\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 3) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872090822000, \"message\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 3, \"origin\": \"jla-02\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 4) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 4, \"origin\": \"jla-02\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 5) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092238000, \"message\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 5, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 6) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 6, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 7) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 7, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 8) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 8, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 9) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 9, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 10) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 10, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 11) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 11, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else if (looper == 12) {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092242000, \"message\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 12, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper++;
                            }
                            else {
                                Assertions
                                        .assertEquals(
                                                "{\"timestamp\": 1650872092243000, \"message\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                                        + partitionCounter
                                                        + "\", \"offset\": 13, \"origin\": \"jla-02.default\"}",
                                                user.toString()
                                        );
                                looper = 0;
                                partitionCounter++;
                            }
                        }

                    }
                }
            }
            LOGGER.debug("Total number of records: " + counter);
            return counter;
        });
    }

    // Deletes the avro-files that were created during testing.
    public void cleanup(Config config, int start, int end) {
        Path queueDirectory = Paths.get(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = start; i <= end; i++) {
                File syslogFile = new File(
                        queueDirectory.toAbsolutePath() + File.separator + "testConsumerTopic" + j + "." + i
                );
                assertDoesNotThrow(() -> {
                    boolean result = Files.deleteIfExists(syslogFile.toPath());
                });
            }
        }
    }
}
