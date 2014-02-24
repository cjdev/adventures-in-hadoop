package io.github.chrisalbright.mapreduce;

import io.github.chrisalbright.utility.WordFunctions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();
        Text fileNameOutput = new Text(fileName + ",1");
        String lineString = line.toString();
        Iterable<String> splitWords = WordFunctions.lineToWords(lineString);
        Iterable<String> trimmedWords = WordFunctions.trimNonWordCharacters(splitWords);
        Iterable<String> words = WordFunctions.lowercaseWords(trimmedWords);

        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(new Text(word), fileNameOutput);
            }
        }

    }
}
