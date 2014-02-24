package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String[] split = line.toString().split("\\|");
        String word = split[0];
        String document = split[1];
        String count = split[2];
        context.write(new Text(document), new Text(word + "|" + count));
    }
}

