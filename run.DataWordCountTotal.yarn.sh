#!/bin/bash

cmd="spark-submit --master yarn-cluster"
cmd="$cmd --num-executors 20 --driver-memory 6g --executor-memory 2g --executor-cores 4"
cmd="$cmd --class voxpopuli.DataWordCountTotal"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd /user/lfischer/voxpopuli/dewiki-latest-pages-articles-txt/*"
cmd="$cmd /user/lfischer/voxpopuli/dewiki-latest-pages-count-total"

echo $cmd
eval $cmd

