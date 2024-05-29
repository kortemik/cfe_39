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
    /* Maps out the latest offset for all the topic partitions available in HDFS.
     The offset map can then be used for kafka consumer seek() method, which will add the idempotent functionality to the consumer.
     Also, because this class should be called outside the loops that generate the consumer groups it should be lightweight to run.*/

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRead.class);
    private final FileSystem fs;
    private final boolean useMockKafkaConsumer; // test-mode switch
    private final Configuration conf;
    private final String hdfsuri;
    private static String topicsRegexString = null;
    private final String path;

    public HDFSRead(Config config) throws IOException {
        // Check if mock kafka consumer is enabled in the config.
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


        }else {
            // Code for initializing the class with kerberos.
            hdfsuri = config.getHdfsuri(); // Get from config.'
            path = config.getHdfsPath();

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

            /* hack for running locally with fake DNS records
             set this to true if overriding the host name in /etc/hosts*/
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
            LOGGER.info("Path <{}> created.", path);
        }

        FileStatus[] directoryStatuses = fs.listStatus(new Path(path), topicFilter);
        // Get the directory statuses. Each directory represents a Kafka topic.
        if (directoryStatuses.length > 0) {
            LOGGER.debug("Found <{}> matching directories", directoryStatuses.length);
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
                        if (offsets.get(topicPartition) < Long.parseLong(offset)+1) {
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
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
