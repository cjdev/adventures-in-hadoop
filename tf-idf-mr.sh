#!/bin/bash

$HADOOP_HOME/bin/hadoop jar tf-idf-mr/target/tf-idf-mr-1.0-SNAPSHOT.jar io.github.chrisalbright.mapreduce.TfIdfDriver -libjars tf-idf-mr/target/tf-idf-mr-1.0-SNAPSHOT-job-dependencies.jar
