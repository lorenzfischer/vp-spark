#!/bin/bash

cmd="spark-submit --master local[4]"
cmd="$cmd --num-executors 1 --driver-memory 4g --executor-memory 5g --executor-cores 2"
cmd="$cmd --class voxpopuli.LdaMuncher"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd --vocabSize 10000 --topicSmoothing 1.1 --termSmoothing 1.1"
cmd="$cmd --stopwordFile file:///Volumes/data/Users/aeon/projects/voxpopuli/vp-spark/src/dev/resources/stopwordlist_de.txt"
#cmd="$cmd file:///Users/lfischer/projects/voxpopuli/word2vec/vp-spark/src/dev/resources/db-dump-small.json"
cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/vp-spark/src/dev/resources/db-dump.small.json"

# lda muncher will interpret all paths as input files.. for now
#cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/vp-spark/munch-output"

echo $cmd
eval $cmd
