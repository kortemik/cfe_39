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

// This is the class for handling the Kafka record topic/partition/offset data that are required for HDFS storage.
public class RecordOffset extends Offset{

    public RecordOffset(
            String topic,
            int partition,
            long offset,
            byte[] record
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.record = record;
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public Integer getPartition() {
        return partition;
    }

    @Override
    public Long getOffset() {
        return offset;
    }

    @Override
    public byte[] getRecord() {
        return record;
    }
}
