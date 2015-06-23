#!/bin/bash

# switch to the directory of this script after storing current working directory
pushd $( dirname "${BASH_SOURCE[0]}" )

# config
source conf.DataMuncher.sh

# yarn config
MUNCHER_INPUT_HDFS=/user/lfischer/voxpopuli/munch-input
MUNCHER_OUTPUT_HDFS=/user/lfischer/voxpopuli/munch-output


# ################# #
# copy data to hdfs #
# ################# #
FULL_NAME=$(basename "$MUNCHER_INPUT")
EXTENSION="${FULL_NAME: -7}"
if [ "$EXTENSION" = ".tar.gz" ]; then
  BASE_NAME="${FULL_NAME%.tar.gz}"

  # use a tmp dir to extract the file
  tmpdir=`mktemp -d -t vp.XXXXXX.`
  tar xzfv $MUNCHER_INPUT -C $tmpdir

  # make sure input dir in HDFS is empty
  # todo: uncomment for live setting, for dev, we safe time like this
  hadoop fs -rm -r -f $MUNCHER_INPUT_HDFS/
  hadoop fs -mkdir $MUNCHER_INPUT_HDFS
  hadoop fs -copyFromLocal $tmpdir/$BASE_NAME/* $MUNCHER_INPUT_HDFS/

  #remove the tmp dir
  rm -rf $tmpdir
else
  hadoop fs -copyFromLocal $MUNCHER_INPUT/* $MUNCHER_INPUT_HDFS/
fi



# #################### #
# run muncher pipeline #
# #################### #

cmd="spark-submit --master yarn-cluster"
cmd="$cmd --num-executors 80 --driver-memory 7g --executor-memory 7g --executor-cores 4"
cmd="$cmd --class voxpopuli.DataMuncher"
cmd="$cmd --deploy-mode cluster" # use one of the worker nodes as the driver machine
cmd="$cmd --files log4j.log-to-remote.properties#log4j.properties" # rename the file to log4j.properties on remote
cmd="$cmd $MUNCHER_JAR"
# don't start more than one task per worker.. it will get too memory hungry.. maybe with count-min-sketch
cmd="$cmd --ldaIterations 100 --ldaTopics 50"
cmd="$cmd --tasks 160 --termWords 5 --retainTop 0.1"
cmd="$cmd $MUNCHER_INPUT_HDFS"
cmd="$cmd $MUNCHER_OUTPUT_HDFS"

echo $cmd
eval $cmd


# #################################### #
# copy data from hdfs to local machine #
# #################################### #

echo "Sleep for 5 seconds to give hdfs time to finish its writing process ..."
sleep 5

# delete the remove file, if it exists already
rm -rf $MUNCHER_OUTPUT
rm -f $MUNCHER_OUTPUT_TAR_FILE
# copy data from hadoop and tar it
hadoop fs -copyToLocal $MUNCHER_OUTPUT_HDFS .
tar czfv $MUNCHER_OUTPUT_TAR_FILE $MUNCHER_OUTPUT

# cd back into the original directory
popd
