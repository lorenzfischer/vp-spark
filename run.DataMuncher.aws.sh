#!/bin/bash


# switch to the directory of this script after storing current working directory
pushd $( dirname "${BASH_SOURCE[0]}" )

# config
source conf.DataMuncher.sh

export ssh_nc='ssh -oStrictHostKeyChecking=no'
export scp_nc='scp -oStrictHostKeyChecking=no'

# ############# #
# start Cluster #
# ############# #
source conf.ec2cluster.sh
export MASTER_NODE=`$SPARK_EC2_DIR/spark-ec2 $SPARK_EC2_BASE_CONFIG get-master $SPARK_CLUSTER_NAME | tail -n 1`

# if the cluster is already running and only start it if it is not already started
if [[ "$MASTER_NODE" == *"Searching for existing cluster"* ]]; then
    echo "Could not find existing Spark cluster. Starting a new one ..."
    source start.ec2cluster.sh
    export MASTER_NODE=`$SPARK_EC2_DIR/spark-ec2 $SPARK_EC2_BASE_CONFIG get-master $SPARK_CLUSTER_NAME | tail -n 1`
    if [[ "$MASTER_NODE" == *"Searching for existing cluster"* ]]; then
        echo "2nd try to start the cluster seems to have failed as well. aborting..."
        exit 1
    fi
fi
#source resume.ec2cluster.sh
echo "spark master node is: $MASTER_NODE"

# ################# #
# copy data to hdfs #
# ################# #

# create ssh tunnel on local machine, so we can connect directly to the master node's hdfs
# we tunnerl from local port 7319 to port 9000 on the master node, as hdfs is running on port 9000 there
# $ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE -N root@$MASTER_NODE -L 7319:localhost:9000

# ok, did not work.. copy the file manually
$scp_nc -i $EC2_MUNCHER_IDENTITY_FILE $MUNCHER_INPUT root@$MASTER_NODE:/root/
FULL_NAME=$(basename "$MUNCHER_INPUT")
EXTENSION="${FULL_NAME: -7}"
if [ "$EXTENSION" = ".tar.gz" ]; then
  BASE_NAME="${FULL_NAME%.tar.gz}"
  $ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE tar xzfv /root/$FULL_NAME -C /root/
  $ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -mkdir /input
  $ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal "/root/$BASE_NAME/*" /input/
  HDFS_INPUT_PATH="hdfs:///input/*"
else
  $ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyFromLocal /root/$FULL_NAME /
  HDFS_INPUT_PATH=hdfs:///$FULL_NAME
fi

#echo "aborting, so you can start the processing manually"
#exit 1

# #################### #
# run muncher pipeline #
# #################### #

# copy the jar to the mast node
$scp_nc -i $EC2_MUNCHER_IDENTITY_FILE $MUNCHER_JAR root@$MASTER_NODE:/root/

cmd="$ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE"
cmd="$cmd /root/spark/bin/spark-submit --master spark://$MASTER_NODE:7077"
cmd="$cmd --num-executors 8 --driver-memory 58g --executor-memory 58g --executor-cores 8"
cmd="$cmd --class voxpopuli.DataMuncher"
cmd="$cmd /root/vp-spark-assembly-1.0.0-SNAPSHOT.jar"
cmd="$cmd --tasks 40 --termWords 6 --minDocFreq 20 --retainTop 0.05"
cmd="$cmd $HDFS_INPUT_PATH"
cmd="$cmd hdfs:///$MUNCHER_OUTPUT"
echo $cmd
eval $cmd


# #################################### #
# copy data from hdfs to local machine #
# #################################### #

# delete the remove file, if it exists already
$ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE rm -f /root/$MUNCHER_OUTPUT_TAR_FILE
$ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE rm -rf /root/$MUNCHER_OUTPUT
$ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE /root/ephemeral-hdfs/bin/hadoop fs -copyToLocal hdfs:///$MUNCHER_OUTPUT /root/
$ssh_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE tar czfv $MUNCHER_OUTPUT_TAR_FILE -C /root/ $MUNCHER_OUTPUT
rm -f $MUNCHER_OUTPUT_TAR_FILE
$scp_nc -i $EC2_MUNCHER_IDENTITY_FILE root@$MASTER_NODE:/root/$MUNCHER_OUTPUT_TAR_FILE .



## ############### #
## destroy cluster #
## ############### #
#./stop.ec2cluster.sh

# cd back into the original directory
popd