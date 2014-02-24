package io.github.chrisalbright;

import io.github.chrisalbright.mapreduce.WordCountMapper;
import io.github.chrisalbright.mapreduce.WordCountReducer;
import io.github.chrisalbright.mapreduce.WordFrequencyMapper;
import io.github.chrisalbright.mapreduce.WordFrequencyReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TfIdfMRTest {
    @Test
    public void testWordFrequencyMapper() throws IOException {

        MapDriver<LongWritable, Text, Text, LongWritable> mapDriver =
          new MapDriver<LongWritable, Text, Text, LongWritable>(new WordFrequencyMapper());

        mapDriver.withInput(new LongWritable(1L), new Text("word1 word5"));
        mapDriver.withInput(new LongWritable(2L), new Text("word2 word6"));
        mapDriver.withInput(new LongWritable(3L), new Text("word3 word7"));
        mapDriver.withInput(new LongWritable(4L), new Text("word4 word8"));

        mapDriver.withOutput(new Text("word1|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word2|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word3|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word4|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word5|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word6|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word7|somefile"), new LongWritable(1L));
        mapDriver.withOutput(new Text("word8|somefile"), new LongWritable(1L));

        mapDriver.runTest(false);
    }

    @Test
    public void testWordFrequencyReducer() throws IOException {
        ReduceDriver<Text, LongWritable, Text, NullWritable> driver =
          new ReduceDriver<Text, LongWritable, Text, NullWritable>(new WordFrequencyReducer());

        driver.withInput(new Text("word1|somefile"), Arrays.asList(new LongWritable(1L), new LongWritable(1L), new LongWritable(1L)));
        driver.withInput(new Text("word2|somefile"), Arrays.asList(new LongWritable(1L), new LongWritable(1L), new LongWritable(1L), new LongWritable(1L)));
        driver.withInput(new Text("word3|somefile"), Arrays.asList(new LongWritable(1L), new LongWritable(1L)));

        driver.withOutput(new Text("word1|somefile|3"), NullWritable.get());
        driver.withOutput(new Text("word2|somefile|4"), NullWritable.get());
        driver.withOutput(new Text("word3|somefile|2"), NullWritable.get());

        driver.runTest();
    }

    @Test
    public void testWordCountMapper() throws IOException {
        MapDriver<LongWritable, Text, Text, Text> driver =
          new MapDriver<LongWritable, Text, Text, Text>(new WordCountMapper());

        driver.withInput(new LongWritable(1), new Text("word1|somefile|3"));
        driver.withInput(new LongWritable(1), new Text("word2|somefile|4"));
        driver.withInput(new LongWritable(1), new Text("word3|somefile|2"));

        driver.withOutput(new Text("somefile"), new Text("word1|3"));
        driver.withOutput(new Text("somefile"), new Text("word2|4"));
        driver.withOutput(new Text("somefile"), new Text("word3|2"));

        driver.runTest(false);
    }

    @Test
    public void testWordCountReducer() throws IOException {
        ReduceDriver<Text, Text, Text, NullWritable> driver =
          new ReduceDriver<Text, Text, Text, NullWritable>(new WordCountReducer());

        driver.withInput(new Text("somefile"), Arrays.asList(new Text("word1|3"), new Text("word2|4"), new Text("word3|2")));

        driver.withOutput(new Text("word1|somefile|3|9"), NullWritable.get());
        driver.withOutput(new Text("word2|somefile|4|9"), NullWritable.get());
        driver.withOutput(new Text("word3|somefile|2|9"), NullWritable.get());

        driver.runTest(false);
    }
}
