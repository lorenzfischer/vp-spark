#!/bin/bash

# ################ #
# stopping cluster #
# ################ #

source conf.ec2cluster.sh

cmd="$SPARK_EC2_DIR/spark-ec2 $SPARK_EC2_BASE_CONFIG stop $SPARK_CLUSTER_NAME"
echo $cmd
eval $cmd
