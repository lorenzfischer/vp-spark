#!/bin/bash

hadoop fs -rm -r /user/lfischer/voxpopuli/dewiki-latest-pages-articles-txt

cmd="spark-submit --master yarn-cluster"
cmd="$cmd --num-executors 10 --driver-memory 6g --executor-memory 2g --executor-cores 4"
cmd="$cmd --class voxpopuli.ConvertWikiXml2Txt"
cmd="$cmd target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd /user/lfischer/voxpopuli/dewiki-latest-pages-articles.xml"
cmd="$cmd /user/lfischer/voxpopuli/dewiki-latest-pages-articles-txt"

echo $cmd
eval $cmd
