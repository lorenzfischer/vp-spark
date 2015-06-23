#!/bin/bash

# ############# #
# start Cluster #
# ############# #

source conf.ec2cluster.sh

# check that host_key checking is disabled for amazon hosts (I know that this is insecure, but I don't have a better
# solution at the moment. todo: find a secure solution to this problem
if grep -Fxq "$FILENAME" ~/.ssh/config
then
    echo "ssh configured correctly"
else
    echo "Put the following into your ~/.ssh/config file and try again:"
    echo ""
    echo "#AWS EC2 public hostnames (changing IPs)
Host *.compute.amazonaws.com
StrictHostKeyChecking no
UserKnownHostsFile /dev/null"
    echo ""
    echo "aborting ..."
    exit 1
fi


echo $SSH_CONFIG

# start cluster
cmd="$SPARK_EC2_DIR/spark-ec2 $SPARK_EC2_BASE_CONFIG $SPARK_EC2_START_CONFIG launch $SPARK_CLUSTER_NAME"
echo $cmd
eval $cmd