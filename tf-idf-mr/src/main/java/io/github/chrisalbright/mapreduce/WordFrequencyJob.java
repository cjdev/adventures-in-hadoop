package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordFrequencyJob extends Configured implements Tool {
    public static void main(String[] args) {
        System.out.println("Hello World!");
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Path input = new Path("/tmp/adventures-in-hadoop/input/*");
        FileStatus[] files = FileSystem.get(conf).globStatus(input);

        conf.setLong("total.docs", files.length);

        Job job = new Job(conf);
        job.setJobName("Word Frequency");

        job.setJarByClass(WordFrequencyJob.class);

        job.setMapperClass(WordFrequencyMapper.class);
        job.setReducerClass(WordFrequencyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, input);

        job.setOutputFormatClass(TextOutputFormat.class);
        Path wordFrequencyDir = new Path("/tmp/adventures-in-hadoop/map-reduce/tf-idf/working/1-word-frequency");
        FileSystem.get(conf).delete(wordFrequencyDir, true);
        TextOutputFormat.setOutputPath(job, wordFrequencyDir);

        return job.waitForCompletion(false) ? 0 : 1;
    }

}
