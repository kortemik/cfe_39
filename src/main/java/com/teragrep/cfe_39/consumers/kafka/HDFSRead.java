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
import org.apache.hadoop.fs.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public final class HDFSRead implements AutoCloseable {
    /* Maps out the latest offset for all the topic partitions available in HDFS.
     The offset map can then be used for kafka consumer seek() method, which will add the idempotent functionality to the consumer.
     Also, because this class should be called outside the loops that generate the consumer groups it should be lightweight to run.*/

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSRead.class);
    private final FileSystem fs;
    private final String path;

    public HDFSRead(Config config, FileSystem fs) throws IOException {
        this.fs = fs;
        path = config.getHdfsPath();
    }

    public Map<TopicPartition, Long> hdfsStartOffsets() throws IOException {
        Map<TopicPartition, Long> offsets = new HashMap<>();

        Path workingDir = fs.getWorkingDirectory();
        Path newDirectoryPath = new Path(path);
        if (!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.info("Path <{}> created.", path);
        }

        FileStatus[] directoryStatuses = fs.listStatus(new Path(path));
        // Get the directory statuses. Each directory represents a Kafka topic.
        if (directoryStatuses.length > 0) {
            LOGGER.debug("Found <{}> matching directories", directoryStatuses.length);
            for (FileStatus directoryStatus : directoryStatuses) {
                // Get the file statuses that are inside the directories.
                FileStatus[] fileStatuses = fs.listStatus(directoryStatus.getPath());
                for (FileStatus fileStatus : fileStatuses) {
                    String topic = fileStatus.getPath().getParent().getName();
                    String[] split = fileStatus.getPath().getName().split("\\."); // The file name can be split to partition parameter and offset parameter. First value is partition and second is offset.
                    String partition = split[0];
                    String offset = split[1];
                    TopicPartition topicPartition = new TopicPartition(topic, Integer.parseInt(partition));
                    if (!offsets.containsKey(topicPartition)) {
                        offsets.put(topicPartition, Long.parseLong(offset) + 1);
                    }
                    else {
                        if (offsets.get(topicPartition) < Long.parseLong(offset) + 1) {
                            offsets.replace(topicPartition, Long.parseLong(offset) + 1);
                        }
                    }
                }
            }
        }
        else {
            LOGGER.info("No matching directories found");
        }
        return offsets;
    }

    // try-with-resources handles closing the filesystem automatically.
    public void close() {
        // NoOp, as closing the FileSystem object here would also close all the other FileSystem objects.
    }
}
