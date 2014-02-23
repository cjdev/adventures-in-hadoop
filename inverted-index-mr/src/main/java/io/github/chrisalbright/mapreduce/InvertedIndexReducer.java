package io.github.chrisalbright.mapreduce;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
  @Override
  protected void reduce(Text word, Iterable<Text> countsPerDocument, Context context) throws IOException, InterruptedException {

    // Keep track of word counts per document
    ConcurrentMap<String, AtomicLong> documentCounts = Maps.newConcurrentMap();

    for (Text documentAndCount : countsPerDocument) {
      String[] split = documentAndCount.toString().split(",");
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

    context.write(word, new Text(output.toString()));
  }
}