package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class InvertedIndexTest {

  @Test
  public void testMap() throws IOException {
    MapDriver<LongWritable, Text, Text, Text> mapDriver =
        new MapDriver<LongWritable, Text, Text, Text>(new InvertedIndexMapper())
            .withInput(new LongWritable(1), new Text("The quick brown fox jumps over the lazy dog"))
            .withOutput(new Text("the"), new Text("somefile,1"))
            .withOutput(new Text("quick"), new Text("somefile,1"))
            .withOutput(new Text("brown"), new Text("somefile,1"))
            .withOutput(new Text("fox"), new Text("somefile,1"))
            .withOutput(new Text("jumps"), new Text("somefile,1"))
            .withOutput(new Text("over"), new Text("somefile,1"))
            .withOutput(new Text("the"), new Text("somefile,1"))
            .withOutput(new Text("lazy"), new Text("somefile,1"))
            .withOutput(new Text("dog"), new Text("somefile,1"));

    boolean checkOutputOrder = false;
    mapDriver.runTest(checkOutputOrder);
  }

  @Test
  public void testReduce() throws IOException {
    ReduceDriver<Text, Text, Text, Text> reduceDriver =
        new ReduceDriver<Text, Text, Text, Text>(new InvertedIndexReducer())
            .withInput(new Text("quick"),
                          Arrays.asList(
                                           new Text("somefile,1"), new Text("somefile,1"), new Text("somefile,1"),
                                           new Text("otherfile,1"), new Text("otherfile,1"), new Text("otherfile,1"), new Text("otherfile,1")))
            .withInput(new Text("brown"),
                          Arrays.asList(
                                           new Text("somefile,1"), new Text("somefile,1"), new Text("somefile,1"),
                                           new Text("nextfile,1"), new Text("nextfile,1"), new Text("nextfile,1"),
                                           new Text("otherfile,1")))
            .withOutput(new Text("brown"), new Text("nextfile:3,somefile:3,otherfile:1"))
            .withOutput(new Text("quick"), new Text("somefile:3,otherfile:4"));

    boolean checkOutputOrder = false;
    reduceDriver.runTest(checkOutputOrder);
  }

  @Test
  public void testMapReduce() throws IOException {
    InvertedIndexMapper mapper = new InvertedIndexMapper();
    InvertedIndexReducer reducer = new InvertedIndexReducer();
    MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> driver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>(mapper, reducer);

    driver.withInput(new LongWritable(1), new Text("The house at the end of the street is brown"));
    driver.withInput(new LongWritable(2), new Text("The quick brown fox jumps over the lazy dog"));
    driver.withInput(new LongWritable(3), new Text("Our house in the middle of our street our house"));

    driver.withOutput(new Text("at"), new Text("somefile:1"));
    driver.withOutput(new Text("brown"), new Text("somefile:2"));
    driver.withOutput(new Text("dog"), new Text("somefile:1"));
    driver.withOutput(new Text("end"), new Text("somefile:1"));
    driver.withOutput(new Text("fox"), new Text("somefile:1"));
    driver.withOutput(new Text("house"), new Text("somefile:3"));
    driver.withOutput(new Text("in"), new Text("somefile:1"));
    driver.withOutput(new Text("is"), new Text("somefile:1"));
    driver.withOutput(new Text("jumps"), new Text("somefile:1"));
    driver.withOutput(new Text("lazy"), new Text("somefile:1"));
    driver.withOutput(new Text("middle"), new Text("somefile:1"));
    driver.withOutput(new Text("of"), new Text("somefile:2"));
    driver.withOutput(new Text("our"), new Text("somefile:3"));
    driver.withOutput(new Text("over"), new Text("somefile:1"));
    driver.withOutput(new Text("quick"), new Text("somefile:1"));
    driver.withOutput(new Text("street"), new Text("somefile:2"));
    driver.withOutput(new Text("the"), new Text("somefile:6"));

    driver.runTest(false);
  }
}
