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
