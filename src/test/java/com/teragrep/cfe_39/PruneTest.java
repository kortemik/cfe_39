package com.teragrep.cfe_39;

import com.teragrep.cfe_39.avro.SyslogRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// Shelved for now. Using HDFS modification timestamps instead of avro-mapred for pruning the files.

// This class should be compiled into a jar-file that is then sent to the hadoop cluster for running the job when needed. Maven should be configured to do the jar-packaging etc.
/*
The records are stored inside files that are 64MB in size and named depending on which Kafka partition offset the last stored record belongs to.
In other words the files are inside topic_name-directory and there are at least one file per partition, depending on the load size of records that are fetched from Kafka topics.

The AVRO-files that hold the records can house over 240,000 records each (at 64MB), but that is just an estimate because the record sizes vary widely.
This means that the partition offsets are not guaranteed to be same for all the files in a topic. In other words the same number of records could be distributed between 3 files on one partition and 2 files on another partition because of the different individual record sizes.
Lets take 2 topic partitions and their files as an example:

topic_name/0.25 (contains partition 0 records between offsets 0 and 25)
topic_name/0.50 (contains partition 0 records between offsets 26 and 50)
topic_name/1.35 (contains partition 1 records between offsets 0 and 35)
topic_name/1.55 (contains partition 1 records between offsets 36 and 55)

Timestamps for the record offsets are linear: record 0.1 is timed first, record 1.1 is second, 0.2 is third, 1.2 is fourth, etc.
Lets say that pruning cutoff epoch lands between records 0.30 and 1.30. This means that the file topic_name/0.25 is pruned but topic_name/1.35 is not.
topic_name/1.35 will contain records from between offsets 0 and 35. Only the records at or above offset 30 are coherent, everything under that can be considered to be garbage data if they are ever queried.

Should the file be altered so the garbage data can be removed from the file? Or should the records only be filtered out when responding to queries with result sets?
The filtering method is most likely the least resource intensive method as it can be done during the MapReduce, and the amount of garbage record shouldn't be too much. If the main function decides on the cutoff epoch, then it can also track it for the datasource function to use for filtering.
But in any case the pruning should include deleting AVRO-files that hold only outdated records that should be pruned. The handling of the leftover garbage records can be handled later in the MapReduce of the datasource component queries.
*/

// The main function that will call for pruning will know the topic name (aka. directory path). The pruning will be done in directory basis, aka. in topic basis, so tracking the topic name is not important for the MapReduce as the input path already contains the topic name.
// Instead, the partition and offset values together with timestamp are important for pruning. The MapReduce function should create a list of key-value pairs where key is the partition+offset and value is the timestamp, where timestamp is smaller than the cutoff_epoch defined by input arguments.
// The pruning of old records can be called in KafkaController.java row 112, using the activeTopics list as a input argument for topic names. This way the records are pruned every time new ones are added. Make sure there are no concurrency issues with the HDFS writer. Most likely there is a need for pruning-controller class that will manage the directory/topic scanning etc.
public class PruneTest extends Configured implements Tool {
    static long cutoff_epoch;
    // TimestampMapper takes a SyslogRecord as input and outputs a key-value pair of record partition+"."+offset and timestamp of the record.
    public static class TimestampMapper extends Mapper<AvroKey<SyslogRecord>, NullWritable, Text, LongWritable> {
        @Override
        public void map(AvroKey<SyslogRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {

            // TODO: Add context.getInputSplit(); functionality to the mapper that allows tracking of the filenames that the records originate from.
            // FIXME: Casting context.getInputSplit()).getPath() to FileSplit shouldn't work anymore with newer versions of hadoop. Input split class now returns TaggedInputSplit instead.
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            // If the FileSplit casting starts working for some reason on the newest version of MapReduce, it can be used to filter out only the specific filenames that should be deleted from HDFS. This way the size of the MapReduce output will be optimized and doesn't need any additional processing by the client.

            long timestamp = key.datum().getTimestamp();
            CharSequence partition = key.datum().getPartition();
            long offset = key.datum().getOffset();
            context.write(new Text(partition + "." + offset), new LongWritable(timestamp)); // Changed the output so the key will contain both the partition and offset (partition + "." + offset), while value will contain the timestamp. This way reduce can do the pruning with the mapper output.
        }
    }


    // Removes all the key-timestamp pairs that have timestamp over the cutoff_epoch. What is left are list of keys (outdated records) that should be removed from the HDFS database.
    public static class TimestampReducer extends Reducer<Text, LongWritable, AvroKey<CharSequence>, AvroValue<Long>> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long timestamp;
            for (LongWritable value : values) {
                timestamp = value.get();
                if (timestamp < cutoff_epoch) { // TODO: inclusive or exclusive? Exclusive for now.
                    context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Long>(timestamp));
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MapReduceTimestampPrune <input path> <output path> <cutoff epoch>");
            return -1;
        }
        try {
            cutoff_epoch = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Usage: 3rd input argument should be parseable to long");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "timestamp prune");
        job.setJarByClass(PruneTest.class);

        SequenceFileInputFormat.setInputPaths(job, new Path(args[0])); // The input path should be the directory where the AVRO-files are held. setInputPaths can take either directory or file as input, not sure if using directory has the same effect as having a list of files.
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output path is where the results of the MapReduce are stored.

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(TimestampMapper.class);
        AvroJob.setInputKeySchema(job, SyslogRecord.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(TimestampReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    // Set input directory to be the topic directory.
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PruneTest(), args); // arg1 is <input path> and arg2 is <output path>, output path should be a new HDFS directory that does not exist and input path should be the HDFS directory with AVRO-files that we have generated in tests.
        System.exit(res);
    }
}
