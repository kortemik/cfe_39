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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSPrune {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSPrune.class);
    private final FileSystem fs;
    private final Path newDirectoryPath;
    private final long cutOffEpoch;

    public HDFSPrune(Config config, String topicName, FileSystem fs) throws IOException {
        this.fs = fs;
        String path = config.getHdfsPath().concat("/").concat(topicName);
        //==== Create directory if not exists
        Path workingDir = fs.getWorkingDirectory();
        newDirectoryPath = new Path(path);
        if (!fs.exists(newDirectoryPath)) {
            // Create new Directory
            fs.mkdirs(newDirectoryPath);
            LOGGER.info("Path <{}> created.", path);
        }
        long pruneOffset = config.getPruneOffset();
        cutOffEpoch = System.currentTimeMillis() - pruneOffset; // pruneOffset is parametrized in Config.java. Default value is 2 days in milliseconds.
    }

    public int prune() throws IOException {
        int deleted = 0;
        // Fetch the filestatuses of HDFS files.
        FileStatus[] fileStatuses = fs.listStatus(new Path(newDirectoryPath + "/"));
        if (fileStatuses.length > 0) {
            for (FileStatus fileStatus : fileStatuses) {
                // Delete old files
                if (fileStatus.getModificationTime() < cutOffEpoch) {
                    boolean delete = fs.delete(fileStatus.getPath(), true);
                    LOGGER.info("Deleted file <{}>", fileStatus.getPath());
                    deleted++;
                }
            }
        }
        else {
            LOGGER.info("No files found in directory <{}>", new Path(newDirectoryPath + "/"));
        }
        return deleted;
    }
}
