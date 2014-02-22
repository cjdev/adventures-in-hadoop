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
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InvertedIndex(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = getConf();

    Job job = new Job(conf);

    job.setInputFormatClass(TextInputFormat.class);
    Path inputDir = new Path("/tmp/adventures-in-hadoop/input/*");
    TextInputFormat.addInputPath(job, inputDir);

    job.setOutputFormatClass(TextOutputFormat.class);
    Path outputDir = new Path("/tmp/adventures-in-hadoop/map-reduce/inverted-index/output");
    FileSystem.get(conf).delete(outputDir, true);
    TextOutputFormat.setOutputPath(job, outputDir);

    job.setMapperClass(InvertedIndexMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(InvertedIndexReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setJarByClass(InvertedIndex.class);
    job.setJobName("Inverted Index (MapReduce)");

    return job.waitForCompletion(false) ? 0 : 1;
  }
}
