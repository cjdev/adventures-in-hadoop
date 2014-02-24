package io.github.chrisalbright.mapreduce;

import com.google.common.collect.Maps;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class WordCountReducer extends Reducer<Text, Text, Text, NullWritable> {

    ConcurrentMap<String, AtomicLong> wordCounter;

    @Override
    protected void reduce(Text document, Iterable<Text> wordCounts, Context context) throws IOException, InterruptedException {
        wordCounter = Maps.newConcurrentMap();
        Long documentWordCount = 0L;
        for (Text wordWithCount : wordCounts) {
            String[] split = wordWithCount.toString().split("\\|");
            String word = split[0];
            Long count = Long.parseLong(split[1]);
            documentWordCount += count;
            wordCounter.put(word, new AtomicLong(0));
            wordCounter.get(word).addAndGet(count);
        }

        for (Map.Entry<String, AtomicLong> entry : wordCounter.entrySet()) {
            context.write(new Text(entry.getKey() + "|" + document + "|" + entry.getValue() + "|" + documentWordCount), NullWritable.get());
        }

    }
}

