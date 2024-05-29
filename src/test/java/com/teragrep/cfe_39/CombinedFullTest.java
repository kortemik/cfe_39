package com.teragrep.cfe_39;

import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.KafkaController;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CombinedFullTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(CombinedFullTest.class);
    private static MiniDFSCluster hdfsCluster;
    private static File baseDir;
    private static Config config;


    // Start minicluster and initialize config.
    @BeforeAll
    public static void startMiniCluster() throws IOException, InterruptedException {
        config = null;
        try {
            config = new Config();
        } catch (IOException e){
            LOGGER.error("Can't load config: {}", e.toString());
            Assertions.fail();
        } catch (IllegalArgumentException e) {
            LOGGER.error("Got invalid config: {}", e.toString());
            Assertions.fail();
        }
        // Create a HDFS miniCluster
        baseDir = Files.createTempDirectory("test_hdfs").toFile().getAbsoluteFile();
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
        config.setHdfsuri(hdfsURI);
        DistributedFileSystem fileSystem = hdfsCluster.getFileSystem();
    }

    // Teardown the minicluster
    @AfterAll
    public static void teardownMiniCluster() {
        hdfsCluster.shutdown();
        FileUtil.fullyDelete(baseDir);
    }

    @Test
    public void kafkaAndAvroFullTest() throws InterruptedException, IOException {
        insertMockFiles(); // Maybe add modifier that allows adding or not adding the mock file.
        config.setMaximumFileSize(3000); // 10 loops (140 records) are in use at the moment, and that is sized at 36,102 bytes.
        KafkaController kafkaController = new KafkaController(config);
        Thread.sleep(10000);
        kafkaController.run();
        /* The avro files should be committed to HDFS now. Check the committed files for any errors.
         There should be 20 files, 10 partitions with each having 2 files assigned to them.
         hdfsReadCheck(); does not work properly if pruning is enabled and prune offset is set too low, which causes the records to be pruned from the database.*/
        if (config.getPruneOffset() == 157784760000L) {
            try {
                hdfsReadCheck();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
    @Test
    public void hdfsPruneTest() throws IOException {
        // Check that the files were properly written to HDFS with a read test.
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath()+"/"+"testConsumerTopic";
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
            LOGGER.info("Path {} created.", path);
        }

        /* Use either HDFS-file modification timestamps or avro-mapred for pruning.

         The records are in this AVRO format:
         {"timestamp": 1650872092240000, "message": "25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]", "directory": "jla02logger", "stream": "test:jla02logger:0", "host": "jla-02.default", "input": "imrelp:cfe-06-0.cfe-06.default:", "partition": "8", "offset": 8, "origin": "jla-02.default"}
         Query handler must be implemented in a way that the AVRO files are first opened, then processed to syslog format and then sent to the query requester. The records are processed/filtered based on the given query conditions using MapReduce to make the code capable of processing the vast amounts of records that are expected.
         MapReduce functionalities of the Hadoop cluster: https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
         Avro side of documentations for MapReduce: https://avro.apache.org/docs/1.11.1/mapreduce-guide/

         Another method for pruning aside using avro-mapred is to use modification timestamp of the avro-file stored in HDFS:
         fs.setTimes(new Path(path+"/"+0.8), Long.parseUnsignedLong("1675930598000"), -1);
         where mtime is modification time and atime is access time. -1 as input parameter leaves the original atime/mtime value as is.*/
        FileStatus[] fileStatuses = fs.listStatus(new Path(newDirectoryPath + "/"));
        long count = Arrays.stream(fileStatuses).count();
        if (count != 0) {
            if (config.getPruneOffset() != 157784760000L) {
                Assertions.fail("There are files available in the database when there should be none.");
            }
            boolean delete = false;
            for (FileStatus a : fileStatuses) {
                // Delete old files
                if (a.getModificationTime() < System.currentTimeMillis()) {
                    delete = fs.delete(a.getPath(), true);
                    Assertions.assertTrue(delete);
                    LOGGER.info("Deleted file {}", a.getPath());
                }
            }
            Assertions.assertTrue(delete);
            LOGGER.info("All files were pruned properly.");
        }else {
            if (config.getPruneOffset() == 157784760000L) {
                Assertions.fail("There were no files available in the database when there should be.");
            }
            LOGGER.info("No files available as they were pruned properly already!");
        }

        fs.close();
    }

    public void hdfsReadCheck() throws IOException {
        // Check that the files were properly written to HDFS with a read test.
        String hdfsuri = config.getHdfsuri();

        String path = config.getHdfsPath()+"/"+"testConsumerTopic";
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
            LOGGER.info("Path {} created.", path);
        }

        /* This is the HDFS write path for the files:
         Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); where newDirectoryPath is config.getHdfsPath() + "/" + lastObject.topic; and filename is lastObject.partition+"."+lastObject.offset;

         Create the list of files to read from HDFS. Test setup is created so each of the 0-9 partitions will have 2 files with offsets of 8 and 13.*/
        List<String> filenameList = new ArrayList<>();
        for (int i = 0; i <= 9; i++) {
            filenameList.add(i + "." + 9);
            filenameList.add(i + "." + 13);
        }
        int looper = 0;
        int partitionCounter = 0;
        for (String fileName : filenameList) {
            //==== Read files
            LOGGER.info("Read file into hdfs");
            //Create a path
            Path hdfsreadpath = new Path(newDirectoryPath + "/" + fileName); // The path should be the same that was used in writing the file to HDFS.
            //Init input stream
            FSDataInputStream inputStream = fs.open(hdfsreadpath);
            //The data is in AVRO-format, so it can't be read as a string.
            DataFileStream<SyslogRecord> reader = new DataFileStream<>(inputStream, new SpecificDatumReader<>(SyslogRecord.class));
            SyslogRecord record = null;
            LOGGER.info("\nReading records from file {}:", hdfsreadpath.toString());
            while (reader.hasNext()) {
                record = reader.next(record);
                LOGGER.debug(record.toString());
                // Assert records here like it is done in KafkaConsumerTest.avroReader().
                if (looper <= 0) {
                    Assertions.assertEquals("{\"timestamp\": 1650872090804000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 0, \"origin\": \"jla-02.default\", \"payload\": \"[WARN] 2022-04-25 07:34:50,804 com.teragrep.jla_02.Log4j Log - Log4j warn says hi!\"}", record.toString());
                    looper++;
                } else if (looper == 1) {
                    Assertions.assertEquals("{\"timestamp\": 1650872090806000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 1, \"origin\": \"jla-02.default\", \"payload\": \"[ERROR] 2022-04-25 07:34:50,806 com.teragrep.jla_02.Log4j Log - Log4j error says hi!\"}", record.toString());
                    looper++;
                } else if (looper == 2) {
                    Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 2, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Daily - Logback-daily says hi.\"}", record.toString());
                    looper++;
                } else if (looper == 3) {
                    Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 3, \"origin\": \"jla-02\", \"payload\": \"470646  [Thread-3] INFO  com.teragrep.jla_02.Logback Audit - Logback-audit says hi.\"}", record.toString());
                    looper++;
                } else if (looper == 4) {
                    Assertions.assertEquals("{\"timestamp\": 1650872090822000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 4, \"origin\": \"jla-02\", \"payload\": \"470647  [Thread-3] INFO  com.teragrep.jla_02.Logback Metric - Logback-metric says hi.\"}", record.toString());
                    looper++;
                } else if (looper == 5) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092238000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 5, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.238 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info audit says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 6) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 6, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info daily says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 7) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092239000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 7, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.239 [INFO] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 info metric says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 8) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 8, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn audit says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 9) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092240000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 9, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.240 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn daily says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 10) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 10, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [WARN] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 warn metric says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 11) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092241000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 11, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.241 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error audit says hi!]\"}", record.toString());
                    looper++;
                } else if (looper == 12) {
                    Assertions.assertEquals("{\"timestamp\": 1650872092242000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 12, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.242 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error daily says hi!]\"}", record.toString());
                    looper++;
                } else {
                    Assertions.assertEquals("{\"timestamp\": 1650872092243000, \"directory\": \"jla02logger\", \"stream\": \"test:jla02logger:0\", \"host\": \"jla-02.default\", \"input\": \"imrelp:cfe-06-0.cfe-06.default:\", \"partition\": \"" + partitionCounter + "\", \"offset\": 13, \"origin\": \"jla-02.default\", \"payload\": \"25.04.2022 07:34:52.243 [ERROR] com.teragrep.jla_02.Log4j2 [instanceId=01, thread=Thread-0, userId=, sessionId=, requestId=, SUBJECT=, VERB=, OBJECT=, OUTCOME=, message=Log4j2 error metric says hi!]\"}", record.toString());
                    looper = 0;
                    LOGGER.info("Partition {} passed assertions.", partitionCounter);
                    partitionCounter++;
                }
            }
            inputStream.close();
        }
        fs.close();
    }

    private void insertMockFiles() throws IOException {
        String path = config.getHdfsPath()+"/"+"testConsumerTopic"; // "hdfs:///opt/teragrep/cfe_39/srv/testConsumerTopic"
        String hdfsURI = config.getHdfsuri();
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsURI);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "hdfs");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS
        FileSystem fs = FileSystem.get(URI.create(hdfsURI), conf);


        //==== Create directory if not exists
        Path workingDir = fs.getWorkingDirectory();
        // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
        Path newDirectoryPath = new Path(path);
        if (!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.debug("Path {} created.", path);
        }

        String dir = System.getProperty("user.dir")+"/src/test/java/com/teragrep/cfe_39/mockHdfsFiles";
        Set<String> listOfFiles = Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
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
            if (fs.exists(hdfswritepath)) {
                throw new RuntimeException("File " + avroFile.getName() + " already exists");
            }
            Path readPath = new Path(avroFile.getPath());
            // Add conditions if file filtering is required for tests.
            fs.copyFromLocalFile(readPath, hdfswritepath);
            LOGGER.debug("End Write file into hdfs");
            LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());
        }
        fs.close();
    }
}
