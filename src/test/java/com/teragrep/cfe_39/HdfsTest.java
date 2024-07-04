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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.teragrep.cfe_39.consumers.kafka.HDFSWrite;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

// Tests the functionality of the HDFSWrite.java.
public class HdfsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;
    private FileSystem fs;

    // Start minicluster and initialize config.
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
    public void hdfsWriteTest() {
        // This test case is for testing the functionality of the HDFSWrite.java by writing pre-generated AVRO-files to the HDFS database and asserting the results are correct.
        assertDoesNotThrow(() -> {
            Assertions.assertFalse(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));

            // writer.commit will delete the file that is given as an input argument. Copy the mock files to another directory so the deletion can be asserted properly too.
            String pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.9";
            java.nio.file.Path sourceFile = Paths.get(pathname);
            java.nio.file.Path targetDir = Paths.get(config.getQueueDirectory());
            java.nio.file.Path targetFile = targetDir.resolve(sourceFile.getFileName());
            Assertions.assertFalse(targetFile.toFile().exists());
            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            File avroFile = new File(targetFile.toUri());
            JsonObject recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(config, recordOffsetJo)) {
                writer.commit(avroFile); // commits avroFile to HDFS and deletes avroFile afterward.
            }
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));

            pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.13";
            sourceFile = Paths.get(pathname);
            targetDir = Paths.get(config.getQueueDirectory());
            targetFile = targetDir.resolve(sourceFile.getFileName());
            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            avroFile = new File(config.getQueueDirectory() + "/0.13");
            recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":13}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(config, recordOffsetJo)) {
                writer.commit(avroFile); // commits avroFile to HDFS and deletes avroFile afterward.
            }
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(2, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.13")));
        });
    }

    @Test
    public void hdfsWriteExceptionTest() {
        // This test case is for testing the functionality of the HDFSWrite.java exception handling by trying to write the same file twice and asserting that the proper exception is thrown.
        assertDoesNotThrow(() -> {
            Assertions.assertFalse(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")));

            // writer.commit will delete the source file that is given as an input argument. Copy the mock file to another directory so the deletion of the source file can be asserted properly.
            String pathname = System.getProperty("user.dir") + "/src/test/resources/mockHdfsFiles/0.9";
            java.nio.file.Path sourceFile = Paths.get(pathname);
            java.nio.file.Path targetDir = Paths.get(config.getQueueDirectory());
            java.nio.file.Path targetFile = targetDir.resolve(sourceFile.getFileName());
            Assertions.assertFalse(targetFile.toFile().exists());
            Files.copy(sourceFile, targetFile);

            Assertions.assertTrue(targetFile.toFile().exists());
            File avroFile = new File(targetFile.toUri());
            JsonObject recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            try (HDFSWrite writer = new HDFSWrite(config, recordOffsetJo)) {
                writer.commit(avroFile); // commits avroFile to HDFS and deletes avroFile afterward.
            }
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));

            Files.copy(sourceFile, targetFile);
            Assertions.assertTrue(targetFile.toFile().exists());
            avroFile = new File(config.getQueueDirectory() + "/0.9");
            recordOffsetJo = JsonParser
                    .parseString("{\"topic\":\"testConsumerTopic\", \"partition\":0, \"offset\":9}")
                    .getAsJsonObject();
            HDFSWrite writer = new HDFSWrite(config, recordOffsetJo);
            File finalAvroFile = avroFile;
            Exception e = Assertions.assertThrows(Exception.class, () -> writer.commit(finalAvroFile));
            Assertions.assertEquals("File 0.9 already exists", e.getMessage());
            writer.close();
            Assertions.assertFalse(targetFile.toFile().exists());
            Assertions
                    .assertEquals(1, fs.listStatus(new Path(config.getHdfsPath() + "/" + "testConsumerTopic")).length);
            Assertions.assertTrue(fs.exists(new Path(config.getHdfsPath() + "/" + "testConsumerTopic" + "/" + "0.9")));
        });
    }
}
