package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = new Job(conf);

        job.setJobName("Word Count");

        job.setJarByClass(WordCountJob.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("/tmp/adventures-in-hadoop/map-reduce/tf-idf/working/1-word-frequency"));

        job.setOutputFormatClass(TextOutputFormat.class);
        Path wordCountDir = new Path("/tmp/adventures-in-hadoop/map-reduce/tf-idf/working/2-word-count");
        FileSystem.get(conf).delete(wordCountDir, true);
        TextOutputFormat.setOutputPath(job, wordCountDir);

        return job.waitForCompletion(false) ? 0 : 1;
    }


}
