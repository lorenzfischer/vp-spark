#!/bin/bash

MUNCHER_JAR=target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar
MUNCHER_INPUT="../db-dump.tar.gz"
MUNCHER_OUTPUT="munch-out"  # the resulting file will be called $MUNCHER_OUTPUT.tar.gz
export SPARK_EC2_START_CONFIG="-t r3.xlarge --spot-price=0.0811 --slaves=4 --hadoop-major-version=1 --ebs-vol-size=2 --spark-git-repo=https://github.com/lorenzfischer/spark -v 583b5a920b4cb57355e5c646777318f9672cd148"

# ############# #
# start Cluster #
# ############# #
# source start.ec2cluster.sh
source resume.ec2cluster.sh
alias remoteSSH="ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE"

# ################# #
# copy data to hdfs #
# ################# #


# copy the file input files to hdfs
scp -i $EC2_MUNCHER_IDENTITY_FILE ../db-dump.json root@$MASTER_NODE:/root/
ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal /root/db-dump.json /
scp -i $EC2_MUNCHER_IDENTITY_FILE src/dev/resources/stopwordlist_de.txt root@$MASTER_NODE:/root/
ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal /root/stopwordlist_de.txt /




# #################### #
# run muncher pipeline #
# #################### #

# copy the jar to the mast node
scp -i $EC2_MUNCHER_IDENTITY_FILE $MUNCHER_JAR root@$MASTER_NODE:/root/

cmd="ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE"
cmd="$cmd /root/spark/bin/spark-submit --master spark://$MASTER_NODE:7077"
cmd="$cmd --num-executors 4 --driver-memory 4g --executor-memory 28g --executor-cores 4"
cmd="$cmd --class voxpopuli.LdaMuncher"
cmd="$cmd /root/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd --vocabSize 15000 --topicSmoothing -1 --termSmoothing -1 --k 100 --maxIterations 30"
cmd="$cmd --stopwordFile hdfs:///stopwordlist_de.txt"
cmd="$cmd hdfs:///db-dump.json"

# lda muncher will interpret all paths as input files.. for now
#cmd="$cmd file:///Volumes/data/Users/aeon/projects/voxpopuli/vp-spark/munch-output"

echo $cmd
eval $cmd

## #################################### #
## copy data from hdfs to local machine #
## #################################### #
#
#ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyToLocal hdfs:///$MUNCHER_OUTPUT /root/
#ssh -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE tar czfv $MUNCHER_OUTPUT.tar.gz /root/$MUNCHER_OUTPUT
## delete the local file, if it exists already
#rm $MUNCHER_OUTPUT.tar.gz
#scp -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE:/root/$MUNCHER_OUTPUT.tar.gz .



## ############### #
## destroy cluster #
## ############### #
#./stop.ec2cluster.sh