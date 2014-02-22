#!/bin/bash

$HADOOP_HOME/bin/hadoop jar inverted-index-mr/target/inverted-index-mr-1.0-SNAPSHOT.jar io.github.chrisalbright.mapreduce.InvertedIndex -libjars inverted-index-mr/target/inverted-index-mr-1.0-SNAPSHOT-job-dependencies.jar