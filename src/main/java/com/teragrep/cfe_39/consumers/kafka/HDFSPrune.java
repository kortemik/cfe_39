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
        this.useMockKafkaConsumer = Boolean
                .parseBoolean(readerKafkaProperties.getProperty("useMockKafkaConsumer", "false"));

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
        }
        else {
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

            /* hack for running locally with fake DNS records
             set this to true if overriding the host name in /etc/hosts*/
            conf.set("dfs.client.use.datanode.hostname", config.getKerberosTestMode());

            /* server principal
             the kerberos principle that the namenode is using*/
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
        }
        else {
            LOGGER.info("No files found in directory <{}>", new Path(newDirectoryPath + "/"));
        }
    }
}
