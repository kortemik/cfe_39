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

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.DatabaseOutput;
import com.teragrep.cfe_39.consumers.kafka.RecordOffset;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// Tests for processing of consumed kafka records with skipping of broken records enabled (both null and non rfc5424).
public class ProcessingTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;
    private FileSystem fs;

    // Prepares known state for testing.
    @BeforeEach
    public void startMiniCluster() {
        assertDoesNotThrow(() -> {
            // Set system properties to use the valid configuration.
            System
                    .setProperty("cfe_39.config.location", System.getProperty("user.dir") + "/src/test/resources/valid.application.properties");
            config = new Config();
            // Create a HDFS miniCluster
            baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
            hdfsCluster = new TestMiniClusterFactory().create(config, baseDir);
            fs = new TestFileSystemFactory().create(config.getHdfsuri());
        });
    }

    // Teardown the minicluster
    @AfterEach
    public void teardownMiniCluster() {
        assertDoesNotThrow(() -> {
            fs.close();
        });
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    @Test
    public void skipNonRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<RecordOffset>> output = new DatabaseOutput(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    "12>1 2022-04-25T07:34:50.806Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            RecordOffset recordOffsetObject = new RecordOffset(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );

            List<RecordOffset> recordOffsetObjectList = new ArrayList<>();
            recordOffsetObjectList.add(recordOffsetObject);
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // File in hdfs does not contain any records, but acts as a marker for kafka consumer offsets.

            // Assert that the file in hdfs contains the expected zero record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertFalse(reader.hasNext());
        });

    }

    @Test
    public void skipNullRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<RecordOffset>> output = new DatabaseOutput(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    null
            );
            RecordOffset recordOffsetObject = new RecordOffset(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );

            List<RecordOffset> recordOffsetObjectList = new ArrayList<>();
            recordOffsetObjectList.add(recordOffsetObject);
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1")));
            // File in hdfs does not contain any records, but acts as a marker for kafka consumer offsets.

            // Assert that the file in hdfs contains the expected zero record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.1");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertFalse(reader.hasNext());
        });

    }

    @Test
    public void skipNullAndNonRFC5424DatabaseOutputTest() {
        // Initialize and register duration statistics
        DurationStatistics durationStatistics = new DurationStatistics();
        durationStatistics.register();

        // register per topic counting
        List<TopicCounter> topicCounters = new CopyOnWriteArrayList<>();

        assertDoesNotThrow(() -> {

            Consumer<List<RecordOffset>> output = new DatabaseOutput(
                    config, // Configuration settings
                    "topicName", // String, the name of the topic
                    durationStatistics, // RuntimeStatistics object from metrics
                    new TopicCounter("topicName") // TopicCounter object from metrics
            );

            List<RecordOffset> recordOffsetObjectList = new ArrayList<>();

            ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    1L,
                    "2022-04-25T07:34:50.806Z".getBytes(StandardCharsets.UTF_8),
                    null
            );
            RecordOffset recordOffsetObject = new RecordOffset(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value()
            );
            recordOffsetObjectList.add(recordOffsetObject);

            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    2L,
                    "2022-04-25T07:34:50.807Z".getBytes(StandardCharsets.UTF_8),
                    "12>1 2022-04-25T07:34:50.807Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new RecordOffset(record.topic(), record.partition(), record.offset(), record.value());
            recordOffsetObjectList.add(recordOffsetObject);
            record = new ConsumerRecord<>(
                    "topicName",
                    0,
                    3L,
                    "2022-04-25T07:34:50.807Z".getBytes(StandardCharsets.UTF_8),
                    "<12>1 2022-04-25T07:34:50.807Z jla-02.default jla02logger - - [origin@48577 hostname=\"jla-02.default\"][event_id@48577 hostname=\"jla-02.default\" uuid=\"c3f13f9a-05e2-41bd-b0ad-1eca6fd6fd9a\" source=\"source\" unixtime=\"1650872090806\"][event_format@48577 original_format=\"rfc5424\"][event_node_relay@48577 hostname=\"cfe-06-0.cfe-06.default\" source=\"kafka-4.kafka.default.svc.cluster.local\" source_module=\"imrelp\"][event_version@48577 major=\"2\" minor=\"2\" hostname=\"cfe-06-0.cfe-06.default\" version_source=\"relay\"][event_node_router@48577 source=\"cfe-06-0.cfe-06.default.svc.cluster.local\" source_module=\"imrelp\" hostname=\"cfe-07-0.cfe-07.default\"][teragrep@48577 streamname=\"test:jla02logger:0\" directory=\"jla02logger\" unixtime=\"1650872090\"] [ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!"
                            .getBytes(StandardCharsets.UTF_8)
            );
            recordOffsetObject = new RecordOffset(record.topic(), record.partition(), record.offset(), record.value());
            recordOffsetObjectList.add(recordOffsetObject);
            output.accept(recordOffsetObjectList);
            Assertions.assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "topicName")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.3")));

            // Assert that the file in hdfs contains the expected single record.

            Path hdfsreadpath = new Path(config.getHdfsPath() + "/" + "topicName" + "/" + "0.3");
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord syslogRecord = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertTrue(reader.hasNext());
            syslogRecord = reader.next(syslogRecord);
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090807000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"0\", \"offset\": 3, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}",
                            syslogRecord.toString()
                    );
            Assertions.assertFalse(reader.hasNext());

        });
    }
}
