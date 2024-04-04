/*
   HDFS Data Ingestion for PTH_06 use CFE-39
   Copyright (C) 2022  Fail-Safe IT Solutions Oy

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.teragrep.cfe_39.consumers.kafka;

import com.teragrep.cfe_39.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class HDFSWriter implements AutoCloseable{

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSWriter.class);
    private final String fileName;
    private final String path;
    private final FileSystem fs;
    private final boolean useMockKafkaConsumer; // test-mode switch
    private final Configuration conf;
    private final String hdfsuri;

    // Create files as whole but stream the contents into them. Avro files 'flush' must be called as few times as possible. Check memory usage impact
    // Later make sure to check the avro file flush issue where the file size is all over the place if flush is not used after every append to the file.

    public HDFSWriter(Config config, RecordOffset lastObject) throws IOException {

        // Check for testmode from config.
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );

        if (useMockKafkaConsumer) {
            // Code for initializing the class in test mode without kerberos.
            hdfsuri = config.getHdfsuri(); // Get from config.

            // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
            // In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
            // These values should be fetched from config and other input parameters (topic+partition+offset).
            path = config.getHdfsPath()+"/"+lastObject.topic;
            fileName = lastObject.partition+"."+lastObject.offset; // filename should be constructed from partition and offset.

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


        }else {
            // Code for initializing the class with kerberos.
            hdfsuri = config.getHdfsuri(); // Get from config.

            // The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
            // In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
            // The values are fetched from config and input parameters (topic+partition+offset).
            path = config.getHdfsPath() + "/" + lastObject.topic; // directory path is constructed from HdfsPath and topic name.
            fileName = lastObject.partition + "." + lastObject.offset; // filename should be constructed from partition and offset.


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

    // Method for committing the AVRO-file to HDFS
    public void commit(File syslogFile, long lastEpochMicros) {
        // The code for writing the file to HDFS should be same for both test (non-kerberized access) and prod (kerberized access).
        if (useMockKafkaConsumer) {
            // CODE FOR TEST-MODE GOES HERE!
            //Get the filesystem - HDFS
            try {
                //==== Create directory if not exists
                Path workingDir = fs.getWorkingDirectory();
                // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
                Path newDirectoryPath = new Path(path);
                if (!fs.exists(newDirectoryPath)) {
                    // Create new Directory
                    fs.mkdirs(newDirectoryPath);
                    LOGGER.debug("Path {} created.", path);
                }

                //==== Write file
                LOGGER.debug("Begin Write file into hdfs");
                //Create a path
                Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
                if (fs.exists(hdfswritepath)) {
                    throw new RuntimeException("File " + fileName + " already exists");
                }

                /*//Init output stream
                FSDataOutputStream outputStream = fs.create(hdfswritepath);
                // Write the file contents of syslogFile to hdfswritepath in HDFS.
                // file to bytes[]

                *//*byte[] bytearray = new byte[(int) syslogFile.length()];
                try (FileInputStream inputStream = new FileInputStream(syslogFile)) {
                    inputStream.read(bytearray);
                }*//*
                byte[] bytes = Files.readAllBytes(Paths.get(syslogFile.getPath())); // if readAllBytes is not efficient use FileInputStream
                outputStream.write(bytes);
                outputStream.close();*/

                Path path = new Path(syslogFile.getPath());
                fs.copyFromLocalFile(path, hdfswritepath);
                // fs.setTimes(hdfswritepath, lastEpochMicros, -1); // where mtime is modification time and atime is access time. -1 as input parameter leaves the original atime/mtime value as is.
                // updateTimestamp(hdfswritepath, lastEpochMicros);
                LOGGER.debug("End Write file into hdfs");
                boolean delete = syslogFile.delete(); // deletes the avro-file from the local disk now that it has been committed to HDFS.
                LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }else {
            //Get the filesystem - HDFS
            try {
                //==== Create directory if not exists
                Path workingDir = fs.getWorkingDirectory();
                // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
                Path newDirectoryPath = new Path(path);
                if (!fs.exists(newDirectoryPath)) {
                    // Create new Directory
                    fs.mkdirs(newDirectoryPath);
                    LOGGER.debug("Path {} created.", path);
                }

                //==== Write file
                LOGGER.debug("Begin Write file into hdfs");
                //Create a path
                Path hdfswritepath = new Path(newDirectoryPath + "/" + fileName); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
                if (fs.exists(hdfswritepath)) {
                    throw new RuntimeException("File " + fileName + " already exists");
                }

                //Init output stream
                FSDataOutputStream outputStream = fs.create(hdfswritepath);
                // Write the file contents of syslogFile to hdfswritepath in HDFS.
                // file to bytes[]

                /*byte[] bytearray = new byte[(int) syslogFile.length()];
                try (FileInputStream inputStream = new FileInputStream(syslogFile)) {
                    inputStream.read(bytearray);
                }*/
                byte[] bytes = Files.readAllBytes(Paths.get(syslogFile.getPath()));
                outputStream.write(bytes);

                outputStream.close();
                LOGGER.debug("End Write file into hdfs");
                boolean delete = syslogFile.delete(); // deletes the avro-file from the local disk now that it has been committed to HDFS.
                LOGGER.debug("\nFile committed to HDFS, file writepath should be: {}\n", hdfswritepath.toString());

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void updateTimestamp(Path hdfswritepath, long lastEpochMicros) {
        // Testing timestamp editing. The new timestamp should be the timestamp of the last record that was added to the AVRO-file.
        try {
            FileSystem fs_temp = FileSystem.get(URI.create(hdfsuri), conf);
            FSDataOutputStream fsDataOutputStream = fs_temp.create(hdfswritepath);
            fs_temp.setTimes(hdfswritepath, lastEpochMicros, -1); // where mtime is modification time and atime is access time. -1 as input parameter leaves the original atime/mtime value as is.
            fsDataOutputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        // FIXME: fs.close() doesn't just affect the current class, it affects all the FileSystem objects that were created using FileSystem.get(URI.create(hdfsuri), conf); in different threads.
        /*if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }*/
    }


}
