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

package com.teragrep.cfe_39.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class DurationStatistics {
    MetricRegistry metricRegistry = new MetricRegistry();
    private static final Logger LOGGER = LoggerFactory.getLogger(DurationStatistics.class);
    private Instant lastReportTime = Instant.now();
    private long lastBytes = 0L;
    private long lastRecords = 0L;
    private final Counter samplingIntervalStat = new Counter();
    private final Meter recordsPerSecondStat = new Meter();
    private final Meter bytesPerSecondStat = new Meter();
    private final Counter records = new Counter();
    private final Counter bytes = new Counter();
    private final Meter threadsStat = new Meter();
    private final Meter bytesStat = new Meter();
    private final Meter recordsStat = new Meter();

    public MetricRegistry register() {
        // Register the different metrics to metricRegistry here.
        metricRegistry.register("samplingIntervalStat", samplingIntervalStat);
        metricRegistry.register("recordsPerSecondStat", recordsPerSecondStat);
        metricRegistry.register("bytesPerSecondStat", bytesPerSecondStat);
        metricRegistry.register("records", records);
        metricRegistry.register("bytes", bytes);
        metricRegistry.register("threadsStat", threadsStat);
        metricRegistry.register("bytesStat", bytesStat);
        metricRegistry.register("recordsStat", recordsStat);
        return metricRegistry;
    }

    public void report() {
        long currentRecords = addAndGetRecords(0); // gets the total number of records processed during the current loop AND the previous loops.
        long currentBytes = addAndGetBytes(0);// gets the total amount of bytes processed during the current loop AND the previous loops.

        // Check if new records were processed
        if (currentRecords > lastRecords) {
            records.inc(currentRecords - lastRecords); // new records found, adding the number of records to records.
        }else {
            long current = records.getCount();
            records.dec(current); // no new records so set the counter back to 0.
        }
        if (currentBytes > lastBytes) {
            bytes.inc(currentBytes - lastBytes); // new records found, adding the number of records to records.
        }else {
            long current = bytes.getCount();
            bytes.dec(current); // no new records so set the counter back to 0.
        }

        Instant currentTime = Instant.now();
        long took = currentTime.toEpochMilli() - lastReportTime.toEpochMilli();
        samplingIntervalStat.inc(took);

        recordsPerSecondStat.mark(currentRecords-lastRecords);
        bytesPerSecondStat.mark(currentBytes-lastBytes);

        // persist
        lastReportTime = currentTime;
        lastRecords = currentRecords;
        lastBytes = currentBytes;
    }

    public long getTotalRecords() {
        return records.getCount();
    }

    public void log() {
        LOGGER.info("## Processed records <{}> and size <{}> KB during <{}> ms / Metrics for the preceding minute: <{}> RPS. <{}> KB/s ", records.getCount(), bytes.getCount() / 1024, samplingIntervalStat.getCount(), recordsPerSecondStat.getOneMinuteRate(), bytesPerSecondStat.getOneMinuteRate() / 1024);
        samplingIntervalStat.dec(samplingIntervalStat.getCount());
    }
    public long addAndGetThreads(long delta) {
        threadsStat.mark(delta);
        return threadsStat.getCount();
    }

    public long addAndGetBytes(long delta) {
        bytesStat.mark(delta);
        return bytesStat.getCount();
    }

    public long addAndGetRecords(long delta) {
        recordsStat.mark(delta);
        return recordsStat.getCount();
    }
}
