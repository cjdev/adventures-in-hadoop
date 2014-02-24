package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class TfIdfJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        Path input = new Path("/tmp/adventures-in-hadoop/input/*");
        FileStatus[] files = FileSystem.get(conf).globStatus(input);

        conf.setLong("total.docs", files.length);

        Job job = new Job(conf);


        job.setJobName("Term Frequency/Inverse Document Frequency");

        job.setJarByClass(TfIdfJob.class);

        job.setMapperClass(TfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/tmp/adventures-in-hadoop/map-reduce/tf-idf/working/2-word-count"));

        job.setOutputFormatClass(TextOutputFormat.class);
        Path outputDir = new Path("/tmp/adventures-in-hadoop/map-reduce/tf-idf/output");
        FileSystem.get(conf).delete(outputDir, true);
        TextOutputFormat.setOutputPath(job, outputDir);


        return job.waitForCompletion(false) ? 0 : 1;
    }
}
