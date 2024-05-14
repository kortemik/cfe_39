package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class HDFSRead implements AutoCloseable {
    // Maps out the latest offset for all the topic partitions available in HDFS.
    // The offset map can then be used for kafka consumer seek() method, which will add the idempotent functionality to the consumer.
    // Also, because this class should be called outside the loops that generate the consumer groups it should be lightweight to run.

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRead.class);
    private final FileSystem fs;
    private final boolean useMockKafkaConsumer; // test-mode switch
    private final Configuration conf;
    private final String hdfsuri;
    private static String topicsRegexString = null;
    private final String path;

    public HDFSRead(Config config) throws IOException {
        // Check for testmode from config.
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );

        if (useMockKafkaConsumer) {
            // Code for initializing the class in test mode without kerberos.
            hdfsuri = config.getHdfsuri(); // Get from config.
            path = config.getHdfsPath();

            // ====== Init HDFS File System Object
            conf = new Configuration();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsuri);
            // Because of Maven
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            // filesystem for HDFS access is set here
            try {
                fs = FileSystem.get(URI.create(hdfsuri), conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            /*//==== Create directory if not exists
            Path workingDir=fs.getWorkingDirectory();
            Path newDirectoryPath= new Path(path);
            if(!fs.exists(newDirectoryPath)) {
                // Create new Directory
                fs.mkdirs(newDirectoryPath);
                LOGGER.info("Path {} created.", path);
            }*/


        }else {
            // Code for initializing the class with kerberos.
            hdfsuri = config.getHdfsuri(); // Get from config.'
            path = config.getHdfsPath();


            // Set HADOOP user here, Kerberus parameters most likely needs to be added here too.
            // System.setProperty("HADOOP_USER_NAME", "hdfs"); // Not needed because user authentication is done by kerberos?
            // System.setProperty("hadoop.home.dir", "/"); // Not needed because user authentication is done by kerberos?

            // set kerberos host and realm
            System.setProperty("java.security.krb5.realm", config.getKerberosRealm());
            System.setProperty("java.security.krb5.kdc", config.getKerberosHost());

            conf = new Configuration();

            // enable kerberus
            conf.set("hadoop.security.authentication", config.getHadoopAuthentication());
            conf.set("hadoop.security.authorization", config.getHadoopAuthorization());

            conf.set("fs.defaultFS", hdfsuri); // Set FileSystem URI
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName()); // Maven stuff?
            conf.set("fs.file.impl", LocalFileSystem.class.getName()); // Maven stuff?

            // hack for running locally with fake DNS records
            // set this to true if overriding the host name in /etc/hosts
            conf.set("dfs.client.use.datanode.hostname", config.getKerberosTestMode());

            // server principal
            // the kerberos principle that the namenode is using
            conf.set("dfs.namenode.kerberos.principal.pattern", config.getKerberosPrincipal());

            // set usergroup stuff
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(config.getKerberosKeytabUser(), config.getKerberosKeytabPath());

            // filesystem for HDFS access is set here
            fs = FileSystem.get(conf);
        }
    }

    public Map<TopicPartition, Long> hdfsStartOffsets() throws IOException {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        if (topicsRegexString == null) {
            topicsRegexString = "^.*$"; // FIXME: all topics if none given
        }

        Path workingDir=fs.getWorkingDirectory();
        Path newDirectoryPath= new Path(path);
        if(!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.info("Path {} created.", path);
        }

        FileStatus[] directoryStatuses = fs.listStatus(new Path(path), topicFilter);
        // Get the directory statuses. Each directory represents a Kafka topic.
        if (directoryStatuses.length > 0) {
            LOGGER.debug("Found {} matching directories", directoryStatuses.length);
            for (FileStatus r : directoryStatuses) {
                // Get the file statuses that are inside the directories.
                FileStatus[] fileStatuses = fs.listStatus(r.getPath());
                for (FileStatus r2 : fileStatuses) {
                    String topic = r2.getPath().getParent().getName();
                    String[] split = r2.getPath().getName().split("\\."); // The file name can be split to partition parameter and offset parameter. First value is partition and second is offset.
                    String partition = split[0];
                    String offset = split[1];
                    TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));
                    if (!offsets.containsKey(topicPartition)) {
                        offsets.put(topicPartition, Long.parseLong(offset)+1);
                    } else {
                        if (offsets.get(topicPartition) < Long.parseLong(offset)) {
                            offsets.replace(topicPartition, Long.parseLong(offset)+1);
                        }
                    }
                }
            }
        }else {
            LOGGER.info("No matching directories found");
        }
        return offsets;
    }

    private static final PathFilter topicFilter = new PathFilter() {
        @Override
        public boolean accept(Path path) {
            return path.getName().matches(topicsRegexString); // Catches the directory names.
        }
    };


    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        // FIXME: fs.close() doesn't just affect the current class, it affects all the FileSystem objects that were created using FileSystem.get(URI.create(hdfsuri), conf); in different threads.
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
