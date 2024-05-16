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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class HDFSPrune {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSPrune.class);
    private Config config;
    private final FileSystem fs;
    private Path newDirectoryPath;
    private long cutOffEpoch;
    private final boolean useMockKafkaConsumer; // test-mode switch

    public HDFSPrune(Config config, String topicName) throws IOException {

        // Check for testmode from config.
        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.useMockKafkaConsumer = Boolean.parseBoolean(
                readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")
        );

        if (useMockKafkaConsumer) {
            this.config = config;
            String hdfsuri = config.getHdfsuri();
            String path = config.getHdfsPath().concat("/").concat(topicName);
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
            fs = FileSystem.get(URI.create(hdfsuri), conf);

            //==== Create directory if not exists
            Path workingDir = fs.getWorkingDirectory();
            newDirectoryPath = new Path(path);
            if (!fs.exists(newDirectoryPath)) {
                // Create new Directory
                fs.mkdirs(newDirectoryPath);
                LOGGER.info("Path <{}> created.", path);
            }
        }else {
            // Code for initializing the class with kerberos.
            String hdfsuri = config.getHdfsuri(); // Get from config.

            String path = config.getHdfsPath() + "/" + topicName;

            // set kerberos host and realm
            System.setProperty("java.security.krb5.realm", config.getKerberosRealm());
            System.setProperty("java.security.krb5.kdc", config.getKerberosHost());

            Configuration conf = new Configuration();

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

            //==== Create directory if not exists
            Path workingDir = fs.getWorkingDirectory();
            newDirectoryPath = new Path(path);
            if (!fs.exists(newDirectoryPath)) {
                // Create new Directory
                fs.mkdirs(newDirectoryPath);
                LOGGER.info("Path <{}> created.", path);
            }
        }
        long pruneOffset = config.getPruneOffset();
        cutOffEpoch = System.currentTimeMillis() - pruneOffset; // pruneOffset is parametrized in Config.java. Default value is 2 days in milliseconds.
    }

    public void prune() throws IOException {
        // Fetch the filestatuses of HDFS files.
        FileStatus[] fileStatuses = fs.listStatus(new Path(newDirectoryPath + "/"));
        if (fileStatuses.length > 0) {
        for (FileStatus a : fileStatuses) {
            // Delete old files
            if (a.getModificationTime() < cutOffEpoch) {
                boolean delete = fs.delete(a.getPath(), true);
                LOGGER.info("Deleted file <{}>", a.getPath());
            }
        }
        } else {
            LOGGER.info("No files found in directory <{}>", new Path(newDirectoryPath + "/"));
        }
    }
}
