#!/bin/bash

cmd="spark-submit --master local[4]"
cmd="$cmd --num-executors 1 --driver-memory 4g --executor-memory 5g --executor-cores 2"
cmd="$cmd --class voxpopuli.CreateWordVectors"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/db-dump.json"
cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/word2vec/vp-spark/munch-output"

echo $cmd
eval $cmd
