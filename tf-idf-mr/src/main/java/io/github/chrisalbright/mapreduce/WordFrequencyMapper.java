package io.github.chrisalbright.mapreduce;

import io.github.chrisalbright.utility.WordFunctions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private long totalDocs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        totalDocs = context.getConfiguration().getLong("total.docs", 0);
    }

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

        InputSplit inputSplit = context.getInputSplit();
        String fileName = "";
        if (inputSplit instanceof FileSplit) {
            fileName = ((FileSplit) inputSplit).getPath().getName();
        }

        Iterable<String> words = WordFunctions.lineToWords(line.toString());

        for (String word : words) {
            context.write(new Text(word + "|" + fileName), new LongWritable(1));
        }
    }
}

