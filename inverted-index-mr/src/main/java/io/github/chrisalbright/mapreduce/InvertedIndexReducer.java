package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    ConcurrentHashMap<String, AtomicLong> documentCounts = new ConcurrentHashMap<String, AtomicLong>();
    documentCounts.clear();
    for (Text value : values) {
      String[] split = value.toString().split(",");
      String document = split[0];
      Long count = new Long(split[1]);
      documentCounts.putIfAbsent(document, new AtomicLong(0));
      documentCounts.get(document).getAndAdd(count);
    }

    StringBuilder output = new StringBuilder();
    for (Map.Entry<String, AtomicLong> documentCount : documentCounts.entrySet()) {
      output
          .append(documentCount.getKey())
          .append(":")
          .append(documentCount.getValue())
          .append(",");
    }

    output.deleteCharAt(output.length()-1);

    context.write(key, new Text(output.toString()));
  }
}