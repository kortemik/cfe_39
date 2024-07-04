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
import com.teragrep.cfe_39.consumers.kafka.HdfsDataIngestion;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion2NewFilesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion2NewFilesTest.class);
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

            // Inserts pre-made avro-files with new timestamps to HDFS, which are normally generated during data ingestion from mock kafka consumer.
            String path = config.getHdfsPath() + "/" + "testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
            // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
            Path newDirectoryPath = new Path(path);
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.debug("Path {} created.", path);
            String dir = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles";
            Set<String> listOfFiles = Stream
                    .of(Objects.requireNonNull(new File(dir).listFiles()))
                    .filter(file -> !file.isDirectory())
                    .map(File::getName)
                    .collect(Collectors.toSet());
            // Loop through all the avro files
            for (String fileName : listOfFiles) {
                String pathname = dir + "/" + fileName;
                File avroFile = new File(pathname);
                //==== Write file
                LOGGER.debug("Begin Write file into hdfs");
                //Create a path
                Path hdfswritepath = new Path(newDirectoryPath + "/" + avroFile.getName()); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
                Assertions.assertFalse(fs.exists(hdfswritepath));
                Path readPath = new Path(avroFile.getPath());
                fs.copyFromLocalFile(readPath, hdfswritepath);
                LOGGER.debug("End Write file into hdfs");
                LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath);
            }
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

    @DisabledIfSystemProperty(
            named = "skipIngestionTest",
            matches = "true"
    )
    @Test
    public void ingestion2NewFilesTest() {
        /* This test case is for testing the functionality of the ingestion when there are files already present in the database before starting ingestion.
        14 records are inserted to HDFS database before starting ingestion, with 124/140 records in mock kafka consumer ready for ingestion.
        Partitions through 1 to 9 will have only a single file, partition 0 will have 2 files (0.9 and 0.13) that are inserted to the database before starting ingestion.
        */
        assertDoesNotThrow(() -> {
            // Assert the known starting state.
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));
            Assertions
                    .assertEquals(2, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(config.getPruneOffset() >= 300000L); // Fails the test if the config is not correct.
            config.setMaximumFileSize(30000);
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            Thread.sleep(10000);
            hdfsDataIngestion.run();

            // Assert that the kafka records were ingested correctly and the database holds the expected 11 files holding the expected 140 records.
            Assertions
                    .assertEquals(11, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "1.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "2.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "3.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "4.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "5.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "6.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "7.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "8.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "9.13")));
        });

        // Check that the files were properly written to HDFS.
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath() + "/" + "testConsumerTopic";
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsuri);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        assertDoesNotThrow(() -> {
            fs = FileSystem.get(URI.create(hdfsuri), conf);

            Path workingDir = fs.getWorkingDirectory();
            Path newDirectoryPath = new Path(path);
            Assertions.assertTrue(fs.exists(newDirectoryPath));

            /* This is the HDFS write path for the files:
             Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); where newDirectoryPath is config.getHdfsPath() + "/" + lastObject.topic; and filename is lastObject.partition+"."+lastObject.offset;
            
             Create the list of files to read from HDFS. Test setup is created so each of the 1-9 partitions will have 1 file with offset of 13, while the 0th partition will have 2 files with offset 9 and 13.*/
            List<String> filenameList = new ArrayList<>();
            filenameList.add("0.9");
            filenameList.add("0.13");
            for (int i = 1; i <= 9; i++) {
                filenameList.add(i + "." + 13);
            }
            FileStatus[] fileStatuses = fs.listStatus(newDirectoryPath);
            Assertions.assertEquals(filenameList.size(), fileStatuses.length);
            for (FileStatus fileStatus : fileStatuses) {
                Assertions.assertTrue(filenameList.contains(fileStatus.getPath().getName()));
            }
            LOGGER.info("All expected files present in HDFS.");

            int partitionCounter = 0;

            // Assertions for file testConsumerTopic/0.9
            String fileName0 = filenameList.get(0);
            Assertions.assertEquals("0.9", fileName0);
            // Assert that file testConsumerTopic/0.9 has expected content.
            LOGGER.info("Read file into hdfs");
            //Create a path
            Path hdfsreadpath = new Path(newDirectoryPath + "/" + fileName0); // The path should be the same that was used in writing the file to HDFS.
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(
                    inputStream,
                    new SpecificDatumReader<>(SyslogRecord.class)
            );
            SyslogRecord record = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090804000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 0, \"origin\": \"jla-02.default\", \"payload\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090806000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 1, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 2, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 3, \"origin\": \"jla-02\", \"payload\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 4, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092238000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 5, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 6, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 7, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 8, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 9, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertFalse(reader.hasNext()); // Reached the end of the testConsumerTopic/0.9 file.
            inputStream.close();
            filenameList.remove(0);

            // Assertions for file testConsumerTopic/0.13
            fileName0 = filenameList.get(0);
            Assertions.assertEquals("0.13", fileName0);
            LOGGER.info("Read file into hdfs");
            //Create a path
            hdfsreadpath = new Path(newDirectoryPath + "/" + fileName0); // The path should be the same that was used in writing the file to HDFS.
            //Init input stream
            inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>(SyslogRecord.class));
            record = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath);

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 10, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 11, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092242000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 12, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\"}",
                            record.toString()
                    );

            Assertions.assertTrue(reader.hasNext());
            record = reader.next(record);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(record.toString());
            }
            Assertions
                    .assertEquals(
                            "{\"timestamp\": 1650872092243000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                    + partitionCounter
                                    + "\", \"offset\": 13, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\"}",
                            record.toString()
                    );
            Assertions.assertFalse(reader.hasNext()); // Reached the end of the testConsumerTopic/0.13 file.
            inputStream.close();
            filenameList.remove(0);

            partitionCounter++;

            for (String fileName : filenameList) {
                //==== Read files
                LOGGER.info("Read file into hdfs");
                //Create a path
                hdfsreadpath = new Path(newDirectoryPath + "/" + fileName); // The path should be the same that was used in writing the file to HDFS.
                //Init input stream
                inputStream = fs.open(hdfsreadpath);
                //The data is in AVRO-format, so it can't be read as a string.
                reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>(SyslogRecord.class));
                record = null;
                LOGGER.info("\nReading records from file {}:", hdfsreadpath);

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090804000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 0, \"origin\": \"jla-02.default\", \"payload\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090806000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 1, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 2, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 3, \"origin\": \"jla-02\", \"payload\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 4, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092238000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 5, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 6, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 7, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 8, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 9, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 10, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 11, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092242000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 12, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\"}",
                                record.toString()
                        );

                Assertions.assertTrue(reader.hasNext());
                record = reader.next(record);
                Assertions
                        .assertEquals(
                                "{\"timestamp\": 1650872092243000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \""
                                        + partitionCounter
                                        + "\", \"offset\": 13, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\"}",
                                record.toString()
                        );
                Assertions.assertFalse(reader.hasNext());
                LOGGER.info("Partition {} passed assertions.", partitionCounter);
                partitionCounter++;
                inputStream.close();
            }
            Assertions.assertEquals(10, partitionCounter);
        });
    }
}
