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

import com.google.gson.*;
import com.teragrep.cfe_39.Config;
import com.teragrep.cfe_39.avro.SyslogRecord;
import com.teragrep.cfe_39.consumers.kafka.queue.WritableQueue;
import com.teragrep.cfe_39.metrics.topic.TopicCounter;
import com.teragrep.cfe_39.metrics.DurationStatistics;
import com.teragrep.rlo_06.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Consumer;

import java.nio.ByteBuffer;

/*  The kafka stream should first be deserialized using rlo_06 and then serialized again using avro and stored in HDFS.
  The target where the record is stored in HDFS is based on the topic, partition and offset. ie. topic_name/0.123456 where offset is 123456
 The mock consumer is activated for testing using the configuration file: readerKafkaProperties.getProperty("useMockKafkaConsumer", "false")*/

public class DatabaseOutput implements Consumer<List<RecordOffset>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseOutput.class);
    private final RFC5424Frame rfc5424Frame = new RFC5424Frame(false);

    private final String table;

    private final DurationStatistics durationStatistics;
    private final TopicCounter topicCounter;

    private long lastTimeCalled = Instant.now().toEpochMilli();

    private SyslogAvroWriter syslogAvroWriter;
    private final long maximumFileSize;
    private final WritableQueue writableQueue;
    private final ByteBuffer sourceConcatenationBuffer;
    private final SDVector teragrepStreamName;
    private final SDVector teragrepDirectory;
    private final SDVector eventNodeSourceSource;
    private final SDVector eventNodeRelaySource;
    private final SDVector eventNodeSourceSourceModule;
    private final SDVector eventNodeRelaySourceModule;
    private final SDVector eventNodeSourceHostname;
    private final SDVector eventNodeRelayHostname;
    private final SDVector originHostname;
    private File syslogFile;
    private final Config config;

    public DatabaseOutput(
            Config config,
            String table,
            DurationStatistics durationStatistics,
            TopicCounter topicCounter
    ) {
        this.config = config;
        this.table = table;
        this.durationStatistics = durationStatistics;
        this.topicCounter = topicCounter;
        this.maximumFileSize = config.getMaximumFileSize();

        // queueDirectory and queueNamePrefix are only used for temporarily storing the AVRO-serialized files before committing them to HDFS when the file size reaches the threshold (or all records are processed).
        this.writableQueue = new WritableQueue(config.getQueueDirectory());

        this.sourceConcatenationBuffer = ByteBuffer.allocateDirect(256 * 1024);
        teragrepStreamName = new SDVector("teragrep@48577", "streamname");
        teragrepDirectory = new SDVector("teragrep@48577", "directory");
        this.eventNodeSourceSource = new SDVector("event_node_source@48577", "source");
        this.eventNodeRelaySource = new SDVector("event_node_relay@48577", "source");
        this.eventNodeSourceSourceModule = new SDVector("event_node_source@48577", "source_module");
        this.eventNodeRelaySourceModule = new SDVector("event_node_relay@48577", "source_module");
        this.eventNodeSourceHostname = new SDVector("event_node_source@48577", "hostname");
        this.eventNodeRelayHostname = new SDVector("event_node_relay@48577", "hostname");
        this.originHostname = new SDVector("origin@48577", "hostname");
    }

    // Checks that the filesize stays under the defined maximum file size. If the file is about to go over target limit commits the file to HDFS and returns true, otherwise does nothing and returns false.
    boolean committedToHdfs(long fileSize, JsonObject recordOffsetObjectJo) {
        try {
            // If the syslogAvroWriter is already initialized, check the filesize so it doesn't go above maximumFileSize.
            if (fileSize > maximumFileSize) {
                // file too large for adding the new record to it, write the still adequately sized AVRO-file to the HDFS database and create a new empty AVRO-file.

                // This part closes the writing of now "complete" AVRO-file and stores the file to HDFS.
                syslogAvroWriter.close();
                try (HDFSWrite writer = new HDFSWrite(config, recordOffsetObjectJo)) {
                    writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
                }

                // This part defines a new empty file to which the new AVRO-serialized records are stored until it again hits the 64M size limit.
                writableQueue
                        .setQueueNamePrefix(recordOffsetObjectJo.get("topic").getAsString() + recordOffsetObjectJo.get("partition").getAsString());
                syslogFile = writableQueue.getNextWritableFile();
                syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                return true;
            }
        }
        catch (IOException ioException) {
            throw new UncheckedIOException(ioException);
        }
        return false;
    }

    private long rfc3339ToEpoch(ZonedDateTime zonedDateTime) {
        final Instant instant = zonedDateTime.toInstant();

        final long MICROS_PER_SECOND = 1000L * 1000L;
        final long NANOS_PER_MICROS = 1000L;
        final long sec = Math.multiplyExact(instant.getEpochSecond(), MICROS_PER_SECOND);

        return Math.addExact(sec, instant.getNano() / NANOS_PER_MICROS);
    }

    /* Input parameter is a list of RecordOffsetObjects. Each object contains a record and its metadata (topic, partition and offset).
     Each partition will get their set of exclusive AVRO-files in HDFS.
     The target where the record is stored in HDFS is based on the topic, partition and last offset. ie. topic_name/0.123456 where last written record's offset is 123456.
     AVRO-file with a path/name that starts with topic_name/0.X should only contain records from the 0th partition of topic named topic_name, topic_name/1.X should only contain records from 1st partition, etc.
     AVRO-files are created dynamically, thus it is not known which record (and its offset) is written to the file last before committing it to HDFS. The final name for the HDFS file is decided only when the file is committed to HDFS.*/
    @Override
    public void accept(List<RecordOffset> recordOffsetObjectList) {
        long thisTime = Instant.now().toEpochMilli();
        long ftook = thisTime - lastTimeCalled;
        topicCounter.setKafkaLatency(ftook);
        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Fuura searching your batch for <[{}]> with records <{}> and took  <{}> milliseconds. <{}> EPS. ",
                            table, recordOffsetObjectList.size(), (ftook),
                            (recordOffsetObjectList.size() * 1000L / ftook)
                    );
        }
        long batchBytes = 0L;

        /*  The recordOffsetObjectList loop will go through all the objects in the list.
          While it goes through the list, the contents of the objects are serialized into an AVRO-file.
          When the file size is about to go above 64M, commit the file into HDFS using the latest topic/partition/offset values as the filename and start fresh with a new empty AVRO-file.
          Serialize the object that was going to make the file go above 64M into the now empty AVRO-file and continue the loop.
         TODO: If the prod-environment recordOffsetObjectList ordering is different from what it is in the test environment, add a function that reorders the list based on partition and offset (or better yet, make several AVRO-files that are being used at the same time rather than doing it one AVRO-file at a time as the offset ordering within partitions should always be correct in all scenarios).*/
        Offset lastObject = new NullOffset(); // Set to null object before initializing as RecordOffsetObject.
        JsonObject lastObjectJo = JsonParser.parseString(lastObject.offsetToJSON()).getAsJsonObject();
        long start = Instant.now().toEpochMilli(); // Starts measuring performance here. Measures how long it takes to process the whole recordOffsetObjectList.
        // This loop goes through all the records of the mock data in a single session.
        for (RecordOffset recordOffsetObject : recordOffsetObjectList) {
            JsonObject recordOffsetObjectJo = JsonParser
                    .parseString(recordOffsetObject.offsetToJSON())
                    .getAsJsonObject();
            // Initializing syslogAvroWriter and lastObject.
            if (syslogAvroWriter == null && lastObject.isNull()) {
                try {
                    writableQueue
                            .setQueueNamePrefix(recordOffsetObjectJo.get("topic").getAsString() + recordOffsetObjectJo.get("partition").getAsString());
                    syslogFile = writableQueue.getNextWritableFile();
                    //  The HDFS filename is only finalized when the AVRO-serialized file is finalized, because every Kafka-record added to the file is going to change the offset that is going to be used for the filename.
                    syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                    lastObject = recordOffsetObject;
                    lastObjectJo = JsonParser.parseString(lastObject.offsetToJSON()).getAsJsonObject();
                }
                catch (IOException ioException) {
                    throw new IllegalArgumentException(ioException);
                }
            }
            else {
                try {
                    if (
                        lastObjectJo.get("topic").getAsString().equals(recordOffsetObjectJo.get("topic").getAsString())
                                & lastObjectJo.get("partition").getAsString().equals(recordOffsetObjectJo.get("partition").getAsString())
                    ) {
                        // Records left to consume in the current partition.
                        boolean fileCommitted = committedToHdfs(syslogAvroWriter.getFileSize(), lastObjectJo);
                        if (fileCommitted) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER
                                        .debug(
                                                "Target file size reached, file <{}> stored to <{}> in HDFS", syslogFile
                                                        .getName(),
                                                lastObjectJo.get("topic").getAsString() + "/" + lastObjectJo.get("partition").getAsString() + "." + lastObjectJo.get("offset").getAsString()
                                        );
                            }
                        }
                        else {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER
                                        .debug(
                                                "Target file size not yet reached, continuing writing records to <{}>.",
                                                syslogFile.getName()
                                        );
                            }
                        }
                    }
                    else {
                        // Previous partition was fully consumed. Commit file to HDFS and create a new AVRO-file.
                        syslogAvroWriter.close();
                        HDFSWrite writer = new HDFSWrite(config, lastObjectJo);
                        writer.commit(syslogFile);

                        // This part defines a new empty file to which the new AVRO-serialized records are stored until it again hits the 64M size limit.
                        writableQueue
                                .setQueueNamePrefix(recordOffsetObjectJo.get("topic").getAsString() + recordOffsetObjectJo.get("partition").getAsString());
                        syslogFile = writableQueue.getNextWritableFile();
                        syslogAvroWriter = new SyslogAvroWriter(syslogFile);
                    }
                }
                catch (IOException ioException) {
                    throw new UncheckedIOException(ioException);
                }
            }

            byte[] byteArray = recordOffsetObject.getRecord(); // loads the byte[] contained in recordOffsetObject.getRecord() to byteArray.
            batchBytes = batchBytes + byteArray.length;
            InputStream inputStream = new ByteArrayInputStream(byteArray);
            rfc5424Frame.load(inputStream);
            try {
                if (rfc5424Frame.next()) {
                    /*                     rfc5424Frame has loaded the record data, it's ready for deserialization.
                      Implement AVRO serialization for the Kafka records here, preparing the data for writing to HDFS.
                      Write all the data into a file using AVRO.
                      The size of each AVRO-serialized file should be as close to 64M as possible.*/

                    // input
                    final byte[] source = eventToSource();

                    // origin
                    final byte[] origin = eventToOrigin();

                    // Format: Use AVRO format with syslog columns as indexed ones
                    final long epochMicros = rfc3339ToEpoch(
                            new RFC5424Timestamp(rfc5424Frame.timestamp).toZonedDateTime()
                    );
                    SyslogRecord syslogRecord = SyslogRecord
                            .newBuilder()
                            .setTimestamp(epochMicros)
                            .setPayload(rfc5424Frame.msg.toString())
                            .setDirectory(rfc5424Frame.structuredData.getValue(teragrepDirectory).toString())
                            .setStream(rfc5424Frame.structuredData.getValue(teragrepStreamName).toString()) // Or is sourcetype/stream supposed to be rfc5424Frame.appName.toString() instead?
                            .setHost(rfc5424Frame.hostname.toString())
                            .setInput(new String(source, StandardCharsets.UTF_8))
                            .setPartition(recordOffsetObjectJo.get("partition").getAsString())
                            .setOffset(recordOffsetObjectJo.get("offset").getAsLong())
                            .setOrigin(new String(origin, StandardCharsets.UTF_8))
                            .build();

                    // Calculate the size of syslogRecord that is going to be written to syslogAvroWriter-file.
                    long capacity = syslogRecord.toByteBuffer().capacity();
                    // Check if there is still room in syslogAvroWriter for another syslogRecord. Commit syslogAvroWriter to HDFS if no room left, emptying it out in the process.
                    boolean fileCommitted = committedToHdfs(syslogAvroWriter.getFileSize() + capacity, lastObjectJo);
                    if (fileCommitted) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER
                                    .debug(
                                            "Target file size reached, file <{}> stored to <{}> in HDFS", syslogFile
                                                    .getName(),
                                            lastObjectJo.get("topic").getAsString() + "/" + lastObjectJo.get("partition").getAsString() + "." + lastObjectJo.get("offset").getAsString()
                                    );
                        }
                    }
                    else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER
                                    .debug(
                                            "Target file size not yet reached, continuing writing records to <{}>.",
                                            syslogFile.getName()
                                    );
                        }
                    }
                    // Add syslogRecord to syslogAvroWriter which has room for new syslogRecord.
                    syslogAvroWriter.write(syslogRecord);
                    lastObject = recordOffsetObject;
                    lastObjectJo = JsonParser.parseString(lastObject.offsetToJSON()).getAsJsonObject();
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        // Handle the "leftover" syslogRecords from the loop.
        try {
            if (syslogAvroWriter != null && !lastObject.isNull()) {
                syslogAvroWriter.close();
                try (HDFSWrite writer = new HDFSWrite(config, lastObjectJo)) {
                    writer.commit(syslogFile); // commits the final AVRO-file to HDFS.
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Measures performance of code that is between start and end.
        long end = Instant.now().toEpochMilli();

        long took = (end - start);
        topicCounter.setDatabaseLatency(took);

        if (took == 0) {
            took = 1;
        }
        long rps = recordOffsetObjectList.size() * 1000L / took;
        topicCounter.setRecordsPerSecond(rps);

        long bps = batchBytes * 1000 / took;
        topicCounter.setBytesPerSecond(bps);

        durationStatistics.addAndGetRecords(recordOffsetObjectList.size());
        durationStatistics.addAndGetBytes(batchBytes);

        topicCounter.addToTotalBytes(batchBytes);
        topicCounter.addToTotalRecords(recordOffsetObjectList.size());

        if (LOGGER.isDebugEnabled()) {
            LOGGER
                    .debug(
                            "Sent batch for <[{}]> with records <{}> and size <{}> KB took <{}> milliseconds. <{}> RPS. <{}> KB/s ",
                            table, recordOffsetObjectList.size(), batchBytes / 1024, (took), rps, bps / 1024
                    );
        }
        lastTimeCalled = Instant.now().toEpochMilli();
    }

    private byte[] eventToOrigin() {
        byte[] origin;
        Fragment originFragment = rfc5424Frame.structuredData.getValue(originHostname);
        if (!originFragment.isStub) {
            origin = originFragment.toBytes();
        }
        else {
            origin = new byte[] {};
        }
        return origin;
    }

    private byte[] eventToSource() {
        /*input is produced from SD element event_node_source@48577 by
         concatenating "source_module:hostname:source". in case
        if event_node_source@48577 is not available use event_node_relay@48577.
        If neither are present, use null value.*/

        sourceConcatenationBuffer.clear();

        Fragment sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSourceModule);
        if (sourceModuleFragment.isStub) {
            sourceModuleFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySourceModule);
        }

        byte[] source_module;
        if (!sourceModuleFragment.isStub) {
            source_module = sourceModuleFragment.toBytes();
        }
        else {
            source_module = new byte[] {};
        }

        Fragment sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceHostname);
        if (sourceHostnameFragment.isStub) {
            sourceHostnameFragment = rfc5424Frame.structuredData.getValue(eventNodeRelayHostname);
        }

        byte[] source_hostname;
        if (!sourceHostnameFragment.isStub) {
            source_hostname = sourceHostnameFragment.toBytes();
        }
        else {
            source_hostname = new byte[] {};
        }

        Fragment sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeSourceSource);
        if (sourceHostnameFragment.isStub) {
            sourceSourceFragment = rfc5424Frame.structuredData.getValue(eventNodeRelaySource);
        }

        byte[] source_source;
        if (!sourceSourceFragment.isStub) {
            source_source = sourceSourceFragment.toBytes();
        }
        else {
            source_source = new byte[] {};
        }

        sourceConcatenationBuffer.put(source_module);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_hostname);
        sourceConcatenationBuffer.put((byte) ':');
        sourceConcatenationBuffer.put(source_source);

        sourceConcatenationBuffer.flip();
        byte[] input = new byte[sourceConcatenationBuffer.remaining()];
        sourceConcatenationBuffer.get(input);

        return input;
    }
}
