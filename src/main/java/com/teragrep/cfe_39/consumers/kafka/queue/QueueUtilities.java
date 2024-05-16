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

package com.teragrep.cfe_39.consumers.kafka.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

class QueueUtilities {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(QueueUtilities.class);

    static BiPredicate<Path, BasicFileAttributes> getFileMatcher(String queueNamePrefix) {
        return (path, basicFileAttributes) -> {
            if (!path.getFileName().toString().startsWith(queueNamePrefix)) {
                return false;
            } else if (path.getFileName().toString().endsWith(".state")) {
                return false;
            } else if (!basicFileAttributes.isRegularFile()) {
                return false;
            } else {
                LOGGER.trace("getFileMatcher returning: <{}>", path);
                return true;
            }
        };
    }

    static void accessCheck(Path queueDirectory) {
        if (!Files.isDirectory(queueDirectory)) {
            throw new IllegalArgumentException(
                    "Provided path is not a "
                            + "directory <[" + queueDirectory + "]>");
        }

        if (!Files.isWritable(queueDirectory)) {
            throw new IllegalArgumentException(
                    "Provided path is not "
                            + "writeable <[" + queueDirectory + "]>");
        }
    }

    static ToLongFunction<Path> getPathToSequenceNumberFunction() {
        return path -> {
            String pathString = path.toString();

            int dotPosition = pathString.lastIndexOf('.');

            String sequenceNumberString = pathString.substring(dotPosition + 1);

            return Long.parseLong(sequenceNumberString);
        };
    }
}