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
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class Ingestion0FilesTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ingestion0FilesTest.class);
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

    @DisabledIfSystemProperty(
            named = "skipIngestionTest",
            matches = "true"
    )
    @Test
    public void ingestion0FilesTest() {
        /*This test case is for testing the functionality of the ingestion when there are no files already present in the database before starting ingestion.
        Maximum file size is set to 30,000 in the config.
        Empty HDFS database, 140 records in mock kafka consumer ready for ingestion. All 14 records for each 10 topic partitions are stored in a single avro-file per partition.*/
        assertDoesNotThrow(() -> {
            Assertions.assertTrue(config.getPruneOffset() >= 300000L); // Fails the test if the config is not correct.
            config.setMaximumFileSize(30000); // This parameter defines the amount of records that can fit inside a single AVRO-file.
            Assertions.assertFalse(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            Thread.sleep(10000);
            hdfsDataIngestion.run();
        });

        // Assert that the kafka records were ingested correctly and the database holds the correct 140 records.
        assertDoesNotThrow(() -> {
            String path = config.getHdfsPath() + "/" + "testConsumerTopic";
            Path newDirectoryPath = new Path(path);
            Assertions.assertTrue(fs.exists(newDirectoryPath));

            /* This is the HDFS write path for the files:
             Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); where newDirectoryPath is config.getHdfsPath() + "/" + lastObject.topic; and filename is lastObject.partition+"."+lastObject.offset;
            
             Create the list of files to read from HDFS. Test setup is created so each of the 0-9 partitions will have 1 file with offset of 13.*/
            List<String> filenameList = new ArrayList<>();
            for (int i = 0; i <= 9; i++) {
                filenameList.add(i + "." + 13);
            }
            FileStatus[] fileStatuses = fs.listStatus(newDirectoryPath);
            Assertions.assertEquals(filenameList.size(), fileStatuses.length);
            for (FileStatus fileStatus : fileStatuses) {
                Assertions.assertTrue(filenameList.contains(fileStatus.getPath().getName()));
            }
            LOGGER.debug("All expected files present in HDFS.");

            int partitionCounter = 0;
            for (String fileName : filenameList) {
                //==== Read files
                LOGGER.info("Read file into hdfs");
                //Create a path
                Path hdfsreadpath = new Path(newDirectoryPath + "/" + fileName); // The path should be the same that was used in writing the file to HDFS.
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

    @DisabledIfSystemProperty(
            named = "skipIngestionTest",
            matches = "true"
    )
    @Test
    public void ingestion0FilesLowSizeTest() {
        /*This test case is for testing the functionality of the ingestion when there are files already present in the database before starting ingestion.
        Maximum file size is set to 3,000 in the config.
        Empty HDFS database, 140 records in mock kafka consumer ready for ingestion. All 14 records for each 10 topic partitions are stored in two avro-files per partition based on MaximumFileSize.*/
        assertDoesNotThrow(() -> {
            Assertions.assertTrue(config.getPruneOffset() >= 300000L); // Fails the test if the config is not correct.
            config.setMaximumFileSize(3000); // This parameter defines the amount of records that can fit inside a single AVRO-file.
            Assertions.assertFalse(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));
            HdfsDataIngestion hdfsDataIngestion = new HdfsDataIngestion(config);
            Thread.sleep(10000);
            hdfsDataIngestion.run();
        });

        // Assert that the kafka records were ingested correctly and the database holds the correct 140 records.

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

            // Assert that the kafka records were ingested correctly and the database holds the expected 20 files.
            Assertions
                    .assertEquals(20, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "1.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "1.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "2.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "2.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "3.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "3.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "4.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "4.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "5.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "5.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "6.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "6.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "7.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "7.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "8.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "8.13")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "9.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "9.13")));
            LOGGER.debug("All expected files present in HDFS.");
        });
    }
}
