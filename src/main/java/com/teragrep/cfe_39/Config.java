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

import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

public class Config {

    private final String queueTopicPattern;
    private final Properties kafkaConsumerProperties;
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
    private final String hdfsPath;
    private String hdfsuri;
    private final String queueDirectory;
    private final String kerberosHost;
    private final String kerberosRealm;
    private final String kerberosPrincipal;
    private final String hadoopAuthentication;
    private final String hadoopAuthorization;
    private final String kerberosKeytabUser;
    private final String kerberosKeytabPath;
    private final String kerberosTestMode;
    private long maximumFileSize;
    private final int numOfConsumers;
    private final long pruneOffset;

    // TODO: Set up configuration check for important parameters.

    public Config() throws IOException {
        Properties properties = new Properties();
        Path configPath = Paths
                .get(System.getProperty("cfe_39.config.location", System.getProperty("user.dir") + "/etc/application.properties"));
        LOGGER.info("Loading application config <[{}]>", configPath.toAbsolutePath());

        try (InputStream inputStream = Files.newInputStream(configPath)) {
            properties.load(inputStream);
            LOGGER.debug("Got configuration: <{}>", properties);
        }

        // HDFS
        this.hdfsPath = properties.getProperty("hdfsPath", "hdfs:///opt/teragrep/cfe_39/srv/");
        this.hdfsuri = properties.getProperty("hdfsuri", "hdfs://localhost:45937/");

        // HDFS pruning
        this.pruneOffset = Long.parseLong(properties.getProperty("pruneOffset", "172800000"));

        // AVRO
        this.queueDirectory = properties.getProperty("queueDirectory", System.getProperty("user.dir") + "/etc/AVRO/");
        this.maximumFileSize = Long.parseLong(properties.getProperty("maximumFileSize", "60800000"));

        // kerberos
        this.kerberosHost = properties.getProperty("java.security.krb5.kdc", "");
        this.kerberosRealm = properties.getProperty("java.security.krb5.realm", "");
        this.hadoopAuthentication = properties.getProperty("hadoop.security.authentication", "");
        this.hadoopAuthorization = properties.getProperty("hadoop.security.authorization", "");
        this.kerberosPrincipal = properties.getProperty("dfs.namenode.kerberos.principal.pattern", "");
        this.kerberosKeytabUser = properties.getProperty("KerberosKeytabUser", "");
        this.kerberosKeytabPath = properties.getProperty("KerberosKeytabPath", "");
        this.kerberosTestMode = properties.getProperty("dfs.client.use.datanode.hostname", "false");

        // kafka
        this.queueTopicPattern = properties.getProperty("queueTopicPattern", "^.*$");
        this.numOfConsumers = Integer.parseInt(properties.getProperty("numOfConsumers", "1"));

        this.kafkaConsumerProperties = loadSubProperties(properties, "consumer.");
        String loginConfig = properties.getProperty("java.security.auth.login.config");
        if (loginConfig == null) {
            throw new IOException("Property java.security.auth.login.config does not exist");
        }
        if (!(new File(loginConfig)).isFile()) {
            throw new IOException("File '" + loginConfig + "' set by java.security.auth.login.config does not exist");
        }
        System.setProperty("java.security.auth.login.config", loginConfig);

        // Just for loggers to work
        Path log4j2Config = Paths
                .get(properties.getProperty("log4j2.configurationFile", System.getProperty("user.dir") + "/etc/log4j2.properties"));
        LOGGER.info("Loading log4j2 config from <[{}]>", log4j2Config.toRealPath());
        Configurator.reconfigure(log4j2Config.toUri());
    }

    private Properties loadSubProperties(Properties properties, String prefix) {
        Properties subProperties = new Properties();

        Enumeration<Object> keys = properties.keys();
        while (keys.hasMoreElements()) {
            String key = String.valueOf(keys.nextElement());
            if (key.startsWith(prefix)) {
                String value = properties.getProperty(key);
                String subKey = key.replaceFirst(prefix, "");
                subProperties.put(subKey, value);
            }
        }
        return subProperties;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsuri(String input) {
        this.hdfsuri = input;
    }

    public String getHdfsuri() {
        return hdfsuri;
    }

    public String getQueueDirectory() {
        return queueDirectory;
    }

    public String getQueueTopicPattern() {
        return queueTopicPattern;
    }

    public Properties getKafkaConsumerProperties() {
        return kafkaConsumerProperties;
    }

    public String getKerberosHost() {
        return kerberosHost;
    }

    public String getKerberosRealm() {
        return kerberosRealm;
    }

    public String getKerberosPrincipal() {
        return kerberosPrincipal;
    }

    public String getHadoopAuthentication() {
        return hadoopAuthentication;
    }

    public String getHadoopAuthorization() {
        return hadoopAuthorization;
    }

    public String getKerberosKeytabUser() {
        return kerberosKeytabUser;
    }

    public String getKerberosKeytabPath() {
        return kerberosKeytabPath;
    }

    public String getKerberosTestMode() {
        return kerberosTestMode;
    }

    public long getMaximumFileSize() {
        return maximumFileSize;
    }

    public void setMaximumFileSize(long maximumFileSize) {
        this.maximumFileSize = maximumFileSize;
    }

    public int getNumOfConsumers() {
        return numOfConsumers;
    }

    public long getPruneOffset() {
        return pruneOffset;
    }
}
