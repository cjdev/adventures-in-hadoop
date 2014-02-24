package io.github.chrisalbright.mapreduce;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordFrequencyReducer extends Reducer<Text, LongWritable, Text, NullWritable> {
    @Override
    protected void reduce(Text wordAndDocument, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
        Long wordCount = 0L;

        for (LongWritable count : counts) {
            wordCount += count.get();
        }

        context.write(new Text(wordAndDocument.toString() + "|" + wordCount), NullWritable.get());

    }
}