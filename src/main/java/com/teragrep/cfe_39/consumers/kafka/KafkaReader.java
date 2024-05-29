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

import org.apache.kafka.clients.consumer.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class KafkaReader implements AutoCloseable {

    final Logger LOGGER = LoggerFactory.getLogger(KafkaReader.class);
    private Iterator<ConsumerRecord<byte[], byte[]>> kafkaRecordsIterator = Collections.emptyIterator();
    private final Consumer<byte[], byte[]> kafkaConsumer;
    private final java.util.function.Consumer<List<RecordOffset>> callbackFunction;

    public KafkaReader(
            Consumer<byte[], byte[]> kafkaConsumer,
            java.util.function.Consumer<List<RecordOffset>> callbackFunction
    ) {
        this.kafkaConsumer = kafkaConsumer;
        this.callbackFunction = callbackFunction;
    }

    public void read() {
        long offset;
        if (!kafkaRecordsIterator.hasNext()) {
            // still need to consume more, infinitely loop because connection problems may cause return of an empty iterator
            ConsumerRecords<byte[], byte[]> kafkaRecords = kafkaConsumer.poll(Duration.ofSeconds(60));
            if (kafkaRecords.isEmpty()) {
                LOGGER.debug("kafkaRecords empty after poll.");
            }
            kafkaRecordsIterator = kafkaRecords.iterator();
        }

        List<RecordOffset> recordOffsetObjectList = new ArrayList<>();
        while (kafkaRecordsIterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = kafkaRecordsIterator.next();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("adding from offset: <{}>", record.offset());
            }
            recordOffsetObjectList
                    .add(new RecordOffset(record.topic(), record.partition(), record.offset(), record.value()));
        }

        if (!recordOffsetObjectList.isEmpty()) {
            /* This is the DatabaseOutput.accept() function.
             Offset and other required data for HDFS storage are added to the input parameters of the accept() function which processes the consumed record.*/
            callbackFunction.accept(recordOffsetObjectList);
            kafkaConsumer.commitSync();
        }
    }

    @Override
    public void close() {
        kafkaConsumer.close(Duration.ofSeconds(60));
    }
}
