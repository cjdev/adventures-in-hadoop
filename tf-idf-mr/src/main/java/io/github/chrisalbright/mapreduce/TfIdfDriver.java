package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.util.ToolRunner;

public class TfIdfDriver {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordFrequencyJob(), args);
        ToolRunner.run(new WordCountJob(), args);
        ToolRunner.run(new TfIdfJob(), args);
    }
}
