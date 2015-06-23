#!/bin/bash

export SPARK_JAVA_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005

cmd="spark-submit --master local[4]"
cmd="$cmd --num-executors 1 --driver-memory 4g --executor-memory 5g --executor-cores 2"
cmd="$cmd --class voxpopuli.DataMuncher"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd --tasks 4 --termWords 6 --minDocFreq 2 --retainTop 0.1"
cmd="$cmd --ldaIterations 10 --ldaTopics 5"
cmd="$cmd file:///Volumes/data/Users/aeon/projectssd/voxpopuli/vp-spark/src/dev/resources/db-dump.small.json"
#cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/vp-spark/src/dev/resources/db-dump.small.json.*.bz2"
cmd="$cmd file:///Volumes/data/Users/aeon/projectssd/voxpopuli/vp-spark/munch-output"

echo $cmd
eval $cmd
