package io.github.chrisalbright.crunch;

import org.apache.crunch.*;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.PTables;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

import static org.apache.crunch.types.avro.Avros.*;

public class InvertedIndexDetailed extends Configured implements Tool, Serializable {


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InvertedIndexDetailed(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Pipeline pipeline = new MRPipeline(InvertedIndexDetailed.class, "Inverted Index (Crunch Implementaion)", getConf());

        Source<String> source = From.textFile("/tmp/adventures-in-hadoop/input/*");
        Target target = To.textFile("/tmp/adventures-in-hadoop/crunch/inverted-index/output");

        // Input: doc1: "words words words"
        // Input: doc2: "go words go"
        // Output: ["words words words", "go words go"]
        PCollection<String> lines =
          pipeline.read(source);

        // Output: [["words words words", "doc1"], ["go words go", "doc2"]]
        PCollection<Pair<String, String>> linesWithDocumentName =
          lines
            .parallelDo("Append file name to each line",
                         InvertedIndexFunctions.linesWithDocumentName,
                         pairs(strings(), strings()));

        // Output: [["words", "doc1"], ["words", "doc1"], ["words", "doc1"], ["go", "doc2"], ["words", "doc2"],["go", "doc2"]]
        PCollection<Pair<String, String>> wordsWithDocumentNames =
          linesWithDocumentName
            .parallelDo("Split lines into words",
                         InvertedIndexFunctions.wordsWithDocumentName,
                         pairs(strings(), strings()));

        // Output: [[["words", "doc1"], 1], [["words", "doc1"], 1], [["words", "doc1"], 1], [["go", "doc2"], 1], [["words", "doc2"], 1], [["go", "doc2"], 1]]
        PTable<Pair<String, String>, Long> wordDocumentCount =
          wordsWithDocumentNames.count();

        // Output: [["words", ["doc1", 1]], ["words", ["doc1", 1]], ["words", ["doc1", 1]], ["go", ["doc2", 1]], ["words", ["doc2", 1]], ["go", ["doc2", 1]]]
        PCollection<Pair<String, Pair<String, Long>>> wordsWithDocumentsAndCounts =
          wordDocumentCount
            .parallelDo("Map word and document with count to word with document and count",
                         InvertedIndexFunctions.wordsWithDocumentsAndCounts,
                         pairs(strings(), pairs(strings(), longs())));


        // Output: [["words", ["doc1", 3]], ["words", ["doc2", 1]], ["go", ["doc2", 2]]]
        PTable<String, Pair<String, Long>> documentAndCountByWord =
          PTables.asPTable(wordsWithDocumentsAndCounts)
            .groupByKey()
            .combineValues(InvertedIndexFunctions.wordCountByDocument);

        // Output: [["words","doc1:3"], ["words","doc2:1"], ["go","doc2:2"]]
        PTable<String, String> output =
          documentAndCountByWord
            .mapValues(
                        "Convert output to text",
                        InvertedIndexFunctions.mapOutputToStrings,
                        strings()
            )
            .groupByKey()
            .combineValues(Aggregators.STRING_CONCAT(",", true));

        output.write(target, Target.WriteMode.OVERWRITE);
        PipelineResult result = pipeline.run();
        return result.succeeded() ? 0 : 1;
    }
}
