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

import com.teragrep.cfe_39.avro.SyslogRecord;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SyncableFileOutputStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

class SyslogAvroWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogAvroWriter.class);

    private final DatumWriter<SyslogRecord> datumWriter =
            new SpecificDatumWriter<>(SyslogRecord.class);

    private final SyncableFileOutputStream syncableFileOutputStream;

    private final DataFileWriter<SyslogRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

    SyslogAvroWriter(File syslogFile) throws IOException {
        dataFileWriter.setCodec(CodecFactory.snappyCodec());


        syncableFileOutputStream =
                new SyncableFileOutputStream(syslogFile);

        syncableFileOutputStream.getChannel().tryLock();

        if (syslogFile.length() == 0) {
            // new file
            dataFileWriter.create(SyslogRecord.getClassSchema(), syncableFileOutputStream);
        }
        else {
            // existing file
            SeekableFileInput seekableFileInput = new SeekableFileInput(syslogFile);

            // seek to end
            syncableFileOutputStream.getChannel().position(syncableFileOutputStream.getChannel().size());
            dataFileWriter.appendTo(seekableFileInput, syncableFileOutputStream);
        }
    }


    void write(SyslogRecord syslogRecord) throws IOException{
        dataFileWriter.append(syslogRecord);
        dataFileWriter.flush();
        // getFileSize() doesn't work properly if dataFileWriter.flush() is not called after appending a new record to the AVRO-file.
    }

    public void close() throws IOException {
        dataFileWriter.close();
    }

    public long getFileSize() throws IOException {
        return syncableFileOutputStream.getChannel().size();
    }
}