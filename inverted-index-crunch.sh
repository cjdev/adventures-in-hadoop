#!/bin/bash

DEPENDENCIES_JAR=$(readlink -f inverted-index-crunch/target/inverted-index-crunch-1.0-SNAPSHOT-job-dependencies.jar)
export HADOOP_CLASSPATH=$DEPENDENCIES_JAR
$HADOOP_HOME/bin/hadoop jar inverted-index-crunch/target/inverted-index-crunch-1.0-SNAPSHOT.jar io.github.chrisalbright.crunch.InvertedIndex -libjars $DEPENDENCIES_JAR
