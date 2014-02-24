package io.github.chrisalbright.crunch;

import com.google.common.collect.Maps;
import io.github.chrisalbright.utility.WordFunctions;
import org.apache.crunch.*;
import org.apache.crunch.impl.mr.run.CrunchInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public final class InvertedIndexFunctions {

    private InvertedIndexFunctions() {
    }

    public static CombineFn<String, Pair<String, Long>> wordCountByDocument =
      new CombineFn<String, Pair<String, Long>>() {

          ConcurrentMap<String, AtomicLong> documentWordCount;

          @Override
          public void process(Pair<String, Iterable<Pair<String, Long>>> input, Emitter<Pair<String, Pair<String, Long>>> emitter) {
              documentWordCount = Maps.newConcurrentMap();
              String word = input.first();
              Iterable<Pair<String, Long>> docsWithCounts = input.second();
              for (Pair<String, Long> docWithCount : docsWithCounts) {
                  String document = docWithCount.first();
                  Long count = docWithCount.second();
                  documentWordCount.putIfAbsent(document, new AtomicLong(0));
                  documentWordCount.get(document).getAndAdd(count);
              }
              for (Map.Entry<String, AtomicLong> entry : documentWordCount.entrySet()) {
                  emitter.emit(new Pair<String, Pair<String, Long>>(word, new Pair<String, Long>(entry.getKey(), entry.getValue().longValue())));
              }
          }
      };

    public static DoFn<String, Pair<String, String>> linesWithDocumentName =
      new DoFn<String, Pair<String, String>>() {

          String inputFile = "";

          @Override
          public void initialize() {
              if (getContext() instanceof MapContext) {

                  InputSplit split = ((MapContext)getContext()).getInputSplit();
                  if (split instanceof CrunchInputSplit) {

                      InputSplit inputSplit = ((CrunchInputSplit) split).getInputSplit();
                      if (inputSplit instanceof FileSplit) {
                          inputFile = ((FileSplit)inputSplit).getPath().getName();
                      }
                  }
              }
          }

          @Override
          public void process(String input, Emitter<Pair<String, String>> emitter) {
              emitter.emit(new Pair<String, String>(input, inputFile));
          }

      };

    public static DoFn<Pair<String, String>, Pair<String, String>> wordsWithDocumentName =
      new DoFn<Pair<String, String>, Pair<String, String>>() {
          @Override
          public void process(Pair<String, String> input, Emitter<Pair<String, String>> emitter) {
              String inputLine = input.first();
              String inputFile = input.second();
              Iterable<String> words = WordFunctions.lineToWords(inputLine);
              Iterable<String> lowercaseWords = WordFunctions.lowercaseWords(words, Locale.US);
              for (String lowercaseWord : lowercaseWords) {
                  emitter.emit(new Pair<String, String>(lowercaseWord, inputFile));
              }
          }
      };

    public static DoFn<Pair<Pair<String, String>, Long>, Pair<String, Pair<String, Long>>> wordsWithDocumentsAndCounts =
      new DoFn<Pair<Pair<String, String>, Long>, Pair<String, Pair<String, Long>>>() {
          @Override
          public void process(Pair<Pair<String, String>, Long> input, Emitter<Pair<String, Pair<String, Long>>> emitter) {
              Pair<String, String> wordAndDoc = input.first();
              String word = wordAndDoc.first();
              String doc = wordAndDoc.second();
              Long wordCount = input.second();

              emitter.emit(new Pair<String, Pair<String, Long>>(word, new Pair<String, Long>(doc, wordCount)));
          }
      };

    public static MapFn<Pair<String, Long>, String> mapOutputToStrings = new MapFn<Pair<String, Long>, String>() {
        @Override
        public String map(Pair<String, Long> input) {
            return input.first() + ":" + input.second().toString();
        }
    };
}
