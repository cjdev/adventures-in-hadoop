package io.github.chrisalbright.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class TfIdfReducer extends Reducer<Text, Text, Text, Text> {

    private long numberOfDocuments;
    private static final DecimalFormat DF = new DecimalFormat("###.########");

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        numberOfDocuments = context.getConfiguration().getLong("total.docs", -1);
    }


    @Override
    protected void reduce(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // total frequency of this word
        int numberOfDocumentsWhereWordAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] split = val.toString().split("\\|");
            numberOfDocumentsWhereWordAppears++;
            tempFrequencies.put(split[0], split[1] + "|" + split[2]);
        }

        for (String document : tempFrequencies.keySet()) {
            String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("\\|");

            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
            double tf = Double.valueOf(wordFrequenceAndTotalWords[0])
                          / Double.valueOf(wordFrequenceAndTotalWords[1]);

            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
            double idf = (double) numberOfDocuments / (double) numberOfDocumentsWhereWordAppears;

            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = numberOfDocuments == numberOfDocumentsWhereWordAppears ?
                             tf : tf * Math.log10(idf);

            context.write(new Text(word + "@" + document), new Text("[" + numberOfDocumentsWhereWordAppears + "/"
                                                                     + numberOfDocuments + " , " + wordFrequenceAndTotalWords[0] + "/"
                                                                     + wordFrequenceAndTotalWords[1] + " , " + DF.format(tfIdf) + "]"));
        }
    }

    protected void reduce0(Text word, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        k:word v:document|wordCount|documentWordCount


        // total frequency of this word
        long numberOfDocumentsWhereKeyAppears = 0L;

        Map<String, String> tempFrequencies = new HashMap<String, String>();


        for (Text val : values) {
            numberOfDocumentsWhereKeyAppears++;
            String[] split = val.toString().split("\\|");

            String document = split[0];
            String wordFrequency = split[1];
            String totalWords = split[2];

            //Term frequency is the quocient of the number of terms in document and the total number of terms in doc
            double tf = Double.valueOf(wordFrequency) / Double.valueOf(totalWords);

            //interse document frequency quocient between the number of docs in corpus and number of docs the term appears
            double idf = (double) numberOfDocuments / (double) numberOfDocumentsWhereKeyAppears;

            //given that log(10) = 0, just consider the term frequency in documents
            double tfIdf = numberOfDocuments == numberOfDocumentsWhereKeyAppears ? tf : tf * Math.log10(idf);

            context.write(new Text(word + "@" + document), new Text("[" + numberOfDocumentsWhereKeyAppears + "/"
                                                                      + numberOfDocuments + " , " + wordFrequency + "/"
                                                                      + totalWords + " , " + DF.format(tfIdf) + "]"));


        }

    }
}
