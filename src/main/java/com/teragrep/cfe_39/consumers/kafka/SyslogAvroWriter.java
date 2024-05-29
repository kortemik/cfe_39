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

class SyslogAvroWriter implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyslogAvroWriter.class);

    private final DatumWriter<SyslogRecord> datumWriter = new SpecificDatumWriter<>(SyslogRecord.class);

    private final SyncableFileOutputStream syncableFileOutputStream;

    private final DataFileWriter<SyslogRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

    SyslogAvroWriter(File syslogFile) throws IOException {
        dataFileWriter.setCodec(CodecFactory.snappyCodec());

        syncableFileOutputStream = new SyncableFileOutputStream(syslogFile);

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

    void write(SyslogRecord syslogRecord) throws IOException {
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
