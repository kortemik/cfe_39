package com.teragrep.cfe_39;

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.HDFSWrite;
import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import com.teragrep.cfe_39.consumers.kafka.RecordOffset;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;

public class HdfsTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsTest.class);

    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;

    // Generate AVRO-files for testing the HDFS writes.
    // @BeforeAll
    public static void generateTestData() throws IOException, InterruptedException {
        config = null;
        try {
            config = new Config();
        } catch (IOException e){
            LOGGER.error("Can't load config: " + e);
            System.exit(1);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Got invalid config: " + e);
            System.exit(1);
        }
        startMiniCluster();
        config.setMaximumFileSize(3000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bytes.
        KafkaController kafkaController = new KafkaController(config);
        kafkaController.run();
    }

    public static void startMiniCluster() throws IOException {
        // Create a HDFS miniCluster
        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        LOGGER.debug("hdfsURI: " + hdfsURI);
        config.setHdfsuri(hdfsURI);
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
    }

    // Delete the generated AVRO-files.
    // @AfterAll
    public static void deleteTestData() throws IOException {
        Path queueDirectory = new Path(config.getQueueDirectory()); // Paths.get(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = 1; i <= 2; i++) {
                File syslogFile = new File(
                        queueDirectory.toUri()
                                + File.separator
                                + "testConsumerTopic"
                                + j
                                + "."
                                + i
                );
                try {
                    boolean result = Files.deleteIfExists(syslogFile.toPath()); //surround it in try catch block
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        closeMiniCluster();
    }

    public static void closeMiniCluster() {
        // Teardown HDFS miniCluster
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    // @Test
    public void miniClusterDebugging() throws InterruptedException, IOException {
        startMiniCluster();
        closeMiniCluster();
    }

    // @Test
    public void hdfsWriteTest() {

        try {
            startMiniCluster();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Path queueDirectory = new Path(config.getQueueDirectory());
        for (int j = 0; j <= 9; j++) {
            for (int i = 1; i <= 2; i++) {
                File syslogFile = new File(
                        queueDirectory.toUri()
                                + File.separator
                                + "testConsumerTopic"
                                + j
                                + "."
                                + i
                );

                // generate lastObject from the last record in the file in this test
                DatumReader<SyslogRecord> userDatumReader = new SpecificDatumReader<>(SyslogRecord.class);
                SyslogRecord lastRecord = null;
                try (DataFileReader<SyslogRecord> dataFileReader = new DataFileReader<>(syslogFile, userDatumReader)) {
                    while (dataFileReader.hasNext()) {
                        lastRecord = dataFileReader.next(lastRecord);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                assert lastRecord != null;
                RecordOffset lastObject = new RecordOffset("testConsumerTopic", Integer.parseInt(lastRecord.getPartition().toString()), lastRecord.getOffset(), null); // Fetch input parameters from the lastRecord SyslogRecord-object.
                LOGGER.debug("\n"+"Last record in the " + syslogFile.getName() + " file:" + "\ntopic: " + lastObject.getTopic() + "\npartition: " + lastObject.getPartition() + "\noffset: " + lastObject.getOffset());
                try (HDFSWrite writer = new HDFSWrite(config, lastObject)) {
                    writer.commit(syslogFile, -1L); // commits the final AVRO-file to HDFS.
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                // Check that the file was stored to HDFS properly.
                try {
                    Thread.sleep(1000);
                    hdfsReadCheck("testConsumerTopic", Integer.parseInt(lastRecord.getPartition().toString()), lastRecord.getOffset());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        closeMiniCluster();
    }

    public void hdfsReadCheck(String testConsumerTopic, int partition, long offset) throws IOException {

        // Check that the files were properly written to HDFS with a read test.
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath()+"/"+testConsumerTopic;
        String fileName = partition+"."+offset;
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
        FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

        //==== Create directory if not exists
        Path workingDir=fs.getWorkingDirectory();
        Path newDirectoryPath= new Path(path);
        if(!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            // logger.info("Path "+path+" created.");
        }

        /*==== Read files
        Create a path*/
        Path hdfsreadpath = new Path(newDirectoryPath + "/" + fileName); // The path should be the same that was used in writing the file to HDFS.
        //Init input stream
        FSDataInputStream inputStream = fs.open(hdfsreadpath);
        //The data is in AVRO-format, so it can't be read as a string.
        DataFileStream<SyslogRecord> reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>(SyslogRecord.class));
        SyslogRecord record = null;
        int looper;
        if (offset == 8) {
            looper = 0;
        } else if (offset == 13) {
            looper = 9;
        }else {
            looper = 0;
            Assertions.fail("The offset of the last record is not 8 or 13, which means a failed test.");
        }
        while (reader.hasNext()) {
            record = reader.next(record);
            LOGGER.debug(record.toString());
            // Assert records here like it is done in KafkaConsumerTest.avroReader().
            if (looper <= 0) {
                Assertions.assertEquals("{\"timestamp\": 1650872090804000, \"message\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 0, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 1) {
                Assertions.assertEquals("{\"timestamp\": 1650872090806000, \"message\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 1, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 2) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 2, \"origin\": \"jla-02\"}", record.toString());
                looper++;
            } else if (looper == 3) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 3, \"origin\": \"jla-02\"}", record.toString());
                looper++;
            } else if (looper == 4) {
                Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"message\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 4, \"origin\": \"jla-02\"}", record.toString());
                looper++;
            } else if (looper == 5) {
                Assertions.assertEquals("{\"timestamp\": 1650872092238000, \"message\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 5, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 6) {
                Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 6, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 7) {
                Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"message\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 7, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 8) {
                Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 8, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 9) {
                Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"message\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 9, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 10) {
                Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 10, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 11) {
                Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"message\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 11, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else if (looper == 12) {
                Assertions.assertEquals("{\"timestamp\": 1650872092242000, \"message\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 12, \"origin\": \"jla-02.default\"}", record.toString());
                looper++;
            } else {
                Assertions.assertEquals("{\"timestamp\": 1650872092243000, \"message\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\", \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partition + "\", \"offset\": 13, \"origin\": \"jla-02.default\"}", record.toString());
                looper = 0;
            }
        }
        inputStream.close();
        fs.close();
    }
}
