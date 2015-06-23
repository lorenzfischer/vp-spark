#!/bin/bash

hadoop fs -rm -r -f /user/lfischer/voxpopuli/dewiki-latest-pages-articles-txt

cmd="spark-submit --master spark://localhost:7077"
cmd="$cmd --num-executors 1 --driver-memory 2g --executor-memory 2g --executor-cores 2"
cmd="$cmd --class voxpopuli.ConvertWikiXml2Txt"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"

echo $cmd
eval $cmd
