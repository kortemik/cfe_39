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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

public class HDFSWrite implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSWrite.class);
    private final String fileName;
    private final String path;
    private final FileSystem fs;
    private final boolean useMockKafkaConsumer; // Defines if mock HDFS database is used for testing
    private final Configuration conf;
    private final String hdfsuri;

    public HDFSWrite(Config config, RecordOffset lastObject) throws IOException {

        Properties readerKafkaProperties = config.getKafkaConsumerProperties();
        this.useMockKafkaConsumer = Boolean
                .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));

        if (useMockKafkaConsumer) {
            // Code for initializing the class for mock hdfs database usage without kerberos.
            hdfsuri = config.getHdfsuri();

            /* The filepath should be something like hdfs:///opt/teragrep/cfe_39/srv/topic_name/0.12345 where 12345 is offset and 0 the partition.
             In other words the directory named topic_name holds files that are named and arranged based on partition and the partition's offset. Every partition has its own set of unique offset values.
             These values should be fetched from config and other input parameters (topic+partition+offset).*/
            path = config.getHdfsPath() + "/" + lastObject.topic;
            fileName = lastObject.partition + "." + lastObject.offset; // filename should be constructed from partition and offset.

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
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

        }
        else {
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
            }
            else {
                LOGGER.info("Path <{}> doesn't exist.", path);
            }

            Path path = new Path(syslogFile.getPath());
            fs.copyFromLocalFile(path, hdfswritepath);
            LOGGER.debug("End Write file into hdfs");
            boolean delete = syslogFile.delete(); // deletes the avro-file from the local disk now that it has been committed to HDFS.
            LOGGER.info("\nFile committed to HDFS, file writepath should be: <{}>\n", hdfswritepath);

        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        /* NoOp
         When used here fs.close() doesn't just affect the current class, it affects all the FileSystem objects that were created using FileSystem.get(URI.create(hdfsuri), conf); in different threads.*/
    }

}
