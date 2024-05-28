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

public class HDFSWrite implements AutoCloseable{

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSWrite.class);
    private final String fileName;
    private final String path;
    private final FileSystem fs;
    private final boolean useMockKafkaConsumer; // Defines if mock HDFS database is used for testing
    private final Configuration conf;
    private final String hdfsuri;

    public HDFSWrite(Config config, RecordOffset lastObject) throws IOException {

        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );

        if (useMockKafkaConsumer) {
            // Code for initializing the class for mock hdfs database usage without kerberos.
            hdfsuri = config.getHdfsuri();

            /* The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
             In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
             These values should be fetched from config and other input parameters (topic+partition+offset).*/
            path = config.getHdfsPath()+"/"+lastObject.topic;
            fileName = lastObject.partition+"."+lastObject.offset; // filename should be constructed from partition and offset.

            // ====== Init HDFS File System Object
            conf = new Configuration();
            // Set FileSystem URI
            conf.set("fs.defaultFS", hdfsuri);
            // Because of Maven
            conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", LocalFileSystem.class.getName());
            // Set HADOOP user here.
            System.setProperty("HADOOP_USER_NAME", "hdfs");
            System.setProperty("hadoop.home.dir", "/");
            // filesystem for HDFS access is set here
            try {
                fs = FileSystem.get(URI.create(hdfsuri), conf);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }else {
            // Code for initializing the class for kerberized HDFS database usage.
            hdfsuri = config.getHdfsuri();

            path = config.getHdfsPath() + "/" + lastObject.topic;
            fileName = lastObject.partition + "." + lastObject.offset;

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

            // hack for running locally with fake DNS records, set this to true if overriding the host name in /etc/hosts
            conf.set("dfs.client.use.datanode.hostname", config.getKerberosTestMode());

            // server principal, the kerberos principle that the namenode is using
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
        try {
            //==== Create directory if not exists
            Path workingDir = fs.getWorkingDirectory();
            // Sets the directory where the data should be stored, if the directory doesn't exist then it's created.
            Path newDirectoryPath = new Path(path);
            if (!fs.exists(newDirectoryPath)) {
                // Create new Directory
                fs.mkdirs(newDirectoryPath);
                LOGGER.info("Path <{}> created.", path);
            }

            //==== Write file
            LOGGER.debug("Begin Write file into hdfs");
            //Create a path
            Path hdfswritepath = new Path(newDirectoryPath.toString() + "/" + fileName); // filename should be set according to the requirements: 0.12345 where 0 is Kafka partition and 12345 is Kafka offset.
            if (fs.exists(hdfswritepath)) {
                throw new RuntimeException("File " + fileName + " already exists");
            } else {
                LOGGER.info("Path <{}> doesn't exist.", path);
            }

            Path path = new Path(syslogFile.getPath());
            fs.copyFromLocalFile(path, hdfswritepath);
            LOGGER.debug("End Write file into hdfs");
            boolean delete = syslogFile.delete(); // deletes the avro-file from the local disk now that it has been committed to HDFS.
            LOGGER.info("\nFile committed to HDFS, file writepath should be: <{}>\n", hdfswritepath);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        /* NoOp
         When used here fs.close() doesn't just affect the current class, it affects all the FileSystem objects that were created using FileSystem.get(URI.create(hdfsuri), conf); in different threads.*/
    }


}
