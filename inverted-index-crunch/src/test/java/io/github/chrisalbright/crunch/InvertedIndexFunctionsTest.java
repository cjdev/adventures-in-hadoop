package io.github.chrisalbright.crunch;

import com.google.common.collect.Iterables;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.emit.InMemoryEmitter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InvertedIndexFunctionsTest {
    @Test
    public void testWordCountByDocument() {
        InMemoryEmitter<Pair<String, Pair<String, Long>>> emitter =
          new InMemoryEmitter<Pair<String, Pair<String, Long>>>();

        Pair<String, Iterable<Pair<String, Long>>> input1 =
          new Pair<String, Iterable<Pair<String, Long>>>("words",
                                                          Arrays.asList(
                                                                         new Pair<String, Long>("doc1", 1L),
                                                                         new Pair<String, Long>("doc1", 1L),
                                                                         new Pair<String, Long>("doc1", 1L),
                                                                         new Pair<String, Long>("doc2", 1L)
                                                          )
          );

        Pair<String, Iterable<Pair<String, Long>>> input2 =
          new Pair<String, Iterable<Pair<String, Long>>>("go",
                                                          Arrays.asList(
                                                                         new Pair<String, Long>("doc2", 1L),
                                                                         new Pair<String, Long>("doc2", 1L)
                                                          )
          );

        InvertedIndexFunctions.wordCountByDocument.process(input1, emitter);
        InvertedIndexFunctions.wordCountByDocument.process(input2, emitter);

        List<Pair<String, Pair<String, Long>>> output = emitter.getOutput();

        assertTrue(Iterables.contains(output, new Pair<String, Pair<String, Long>>("words", new Pair<String, Long>("doc1", 3L))));
        assertTrue(Iterables.contains(output, new Pair<String, Pair<String, Long>>("words", new Pair<String, Long>("doc2", 1L))));
        assertTrue(Iterables.contains(output, new Pair<String, Pair<String, Long>>("go", new Pair<String, Long>("doc2", 2L))));
    }

    @Ignore
    @Test
    public void testAppendDocumentName() {
//        MapContext context = mock(MapContext.class);
//        CrunchInputSplit split = mock(CrunchInputSplit.class);
//        FileSplit fileSplit = mock(FileSplit.class);
//        String line = "the quick brown fox jumps over the lazy dog";
//        String fileName = "mockfile.txt";
//        String pathString = "/tmp/" + fileName;
//
//        when(fileSplit.getPath()).thenReturn(new Path(pathString));
//        when(split.getInputSplit()).thenReturn(fileSplit);
//        when(context.getInputSplit()).thenReturn(split);
//
//        InMemoryEmitter<Pair<String, String>> emitter =
//          new InMemoryEmitter<Pair<String, String>>();
//
//        InvertedIndexFunctions.linesWithDocumentName.setContext(context);
//        InvertedIndexFunctions.linesWithDocumentName.initialize();
//
//        InvertedIndexFunctions.linesWithDocumentName.process(line, emitter);
//
//        List<Pair<String, String>> output = emitter.getOutput();
//
//        Pair<String, String> record = output.get(0);
//
//        assertThat(record.first(), is(line));
//        assertThat(record.second(), is(fileName));


    }

    @Test
    public void testWordsWithDocumentName() {

        String line = "The quick brown fox jumps over the lazy dog";
        String fileName = "mockfile.txt";

        InMemoryEmitter<Pair<String, String>> emitter =
          new InMemoryEmitter<Pair<String, String>>();


        InvertedIndexFunctions.wordsWithDocumentName.process(new Pair<String, String>(line, fileName), emitter);

        List<Pair<String, String>> output = emitter.getOutput();

        assertFalse(Iterables.contains(output, new Pair<String, String>("The", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("the", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("quick", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("brown", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("fox", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("jumps", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("over", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("lazy", "mockfile.txt")));
        assertTrue(Iterables.contains(output, new Pair<String, String>("dog", "mockfile.txt")));
        assertThat(output.size(), is(9));
    }

    @Test
    public void testWordsWithDocumentsAndCounts() {

        InMemoryEmitter<Pair<String, Pair<String, Long>>> emitter =
          new InMemoryEmitter<Pair<String, Pair<String, Long>>>();

        Pair<Pair<String, String>, Long> input = new Pair<Pair<String, String>, Long>(new Pair<String, String>("word", "doc"), 7L);

        InvertedIndexFunctions.wordsWithDocumentsAndCounts.process(input, emitter);

        List<Pair<String, Pair<String, Long>>> output = emitter.getOutput();

        Pair<String, Pair<String, Long>> record1 = output.get(0);

        assertThat(record1.first(), is("word"));
        assertThat(record1.second(), is(new Pair<String, Long>("doc", 7L)));
    }

    @Test
    public void testMapOutputToStrings() {
        assertThat(InvertedIndexFunctions.mapOutputToStrings.map(new Pair<String, Long>("document", 99l)), is("document:99"));
    }
}
