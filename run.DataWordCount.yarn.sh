#!/bin/bash

cmd="spark-submit --master yarn-cluster"
cmd="$cmd --num-executors 20 --driver-memory 6g --executor-memory 2g --executor-cores 4"
cmd="$cmd --class voxpopuli.DataWordCount"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd /user/lfischer/voxpopuli/dewiki-latest-pages-articles.1000.xml"
cmd="$cmd /user/lfischer/voxpopuli/german-books-counts"

echo $cmd
eval $cmd
