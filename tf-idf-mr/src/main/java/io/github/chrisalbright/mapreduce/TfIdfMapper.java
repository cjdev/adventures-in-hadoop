package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TfIdfMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\\|");

        String word = split[0];
        String document = split[1];
        String wordCount = split[2];
        String wordsInDocument = split[3];

        context.write(new Text(word), new Text(document + "|" + wordCount + "|" + wordsInDocument));
    }
}
