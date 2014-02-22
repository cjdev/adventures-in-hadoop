package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Locale;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    FileSplit inputSplit = (FileSplit) context.getInputSplit();
    String fileName = inputSplit.getPath().getName();
    Text fileNameOutput = new Text(fileName + ",1");
    String line = value.toString();
    String strippedLine = line.replaceAll("\\W", " ");
    String[] tokens = strippedLine.split("\\s");

    for (String token : tokens) {
      context.write(new Text(token.toLowerCase(Locale.US)), fileNameOutput);
    }

  }
}
