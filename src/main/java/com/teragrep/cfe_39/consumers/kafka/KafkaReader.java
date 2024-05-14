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
            Consumer<byte[], byte[]> kafkaConsumer, java.util.function.Consumer<List<RecordOffset>> callbackFunction) {
        this.kafkaConsumer = kafkaConsumer;
        this.callbackFunction = callbackFunction;
    }

    public void read() {
        long offset;
        if (!kafkaRecordsIterator.hasNext()) {
            // still need to consume more, infinitely loop because connection problems may cause return of an empty iterator
            ConsumerRecords<byte[], byte[]> kafkaRecords = kafkaConsumer.poll(Duration.ofSeconds(60)); // TODO parametrize
            if (kafkaRecords.isEmpty()) {
                LOGGER.debug("kafkaRecords empty after poll.");
            }
            kafkaRecordsIterator = kafkaRecords.iterator();
        }

        List<RecordOffset> recordOffsetObjectList = new ArrayList<>();
        while (kafkaRecordsIterator.hasNext()) {
            ConsumerRecord<byte[], byte[]> record = kafkaRecordsIterator.next();
            LOGGER.debug("adding from offset: " + record.offset());
            recordOffsetObjectList.add(new RecordOffset(record.topic(), record.partition(), record.offset(), record.value()));
        }

        if (!recordOffsetObjectList.isEmpty()) {
            // This is the DatabaseOutput.accept() function.
            // Offset and other required data for HDFS storage are added to the input parameters of the accept() function which processes the consumed record.
            callbackFunction.accept(recordOffsetObjectList);
            kafkaConsumer.commitSync();
            /*
            commitSync() only commits the offsets that were actually polled and processed. If some offsets were not included in the last poll, then those offsets will not be committed.
            It will not commit the latest positions for all subscribed partitions. This would interfere with the Consumer Offset management concept of Kafka to be able to re-start an application where it left off.
            * */
        }
    }

    @Override
    public void close() {
        kafkaConsumer.close(Duration.ofSeconds(60)); // TODO parametrize
    }
}
