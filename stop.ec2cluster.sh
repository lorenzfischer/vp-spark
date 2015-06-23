#!/bin/bash

# switch to the directory of this script after storing current working directory
pushd $( dirname "${BASH_SOURCE[0]}" )

# ############### #
# destroy cluster #
# ############### #

source conf.ec2cluster.sh

cmd="$SPARK_EC2_DIR/spark-ec2 $SPARK_EC2_BASE_CONFIG destroy $SPARK_CLUSTER_NAME"
echo $cmd
eval $cmd

# cd back into the original directory
popd