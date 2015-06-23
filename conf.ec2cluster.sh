#!/bin/bash

export SPARK_EC2_DIR="$SPARK_HOME/ec2"
export SPARK_CLUSTER_NAME="vp-spark-cluster"
#export SPARK_EC2_START_CONFIG="-t r3.xlarge --spot-price=0.0811 --slaves=4 --hadoop-major-version=1 --ebs-vol-size=100 --spark-git-repo=https://github.com/lorenzfischer/spark -v 35e23ff140cd65a4121e769ee0a4e22a3490be37"
#export SPARK_EC2_START_CONFIG="-t r3.xlarge --spot-price=0.0811 --slaves=4 --hadoop-major-version=1 --ebs-vol-size=100 --spark-git-repo=https://github.com/lorenzfischer/spark -v 35e23ff140cd65a4121e769ee0a4e22a3490be37"
export SPARK_EC2_START_CONFIG="-t r3.2xlarge --spot-price=0.2111 --slaves=8 --hadoop-major-version=1 --ebs-vol-size=10 -v 1.3.0"

# this method checks for the existance of the environment variable names that are supplied as an array in
# the first parameter to the function. It then prints all missing parameters and aborts script execution
# if there any variables could not be found.
function verifyVariables {
    VAR_NAMES=$1
    for VAR_NAME in "${VAR_NAMES[@]}"; do
       if [[ -z "${!VAR_NAME}" ]]; then echo "$VAR_NAME is not set!"; VARIABLES_MISSING=1; fi
    done
    if [[ -n $VARIABLES_MISSING ]]; then
        exit 1
    fi
}

# execute setup file, this file needs to set the following variables:
#
# export KEY_PAIR="vp-management"
# export IDENTITY_FILE="/Volumes/data/Users/aeon/.ec2/vp-management.pem"
# export AWS_ACCESS_KEY_ID="ABCDEFG1234567890123"
# export AWS_SECRET_ACCESS_KEY="AaBbCcDdEeFGgHhIiJjKkLlMmNnOoPpQqRrSsTtU"
source ~/.ec2/setup.sh
VAR_NAMES=("SPARK_EC2_START_CONFIG" "EC2_MUNCHER_IDENTITY_FILE" "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "KEY_PAIR" "SPARK_HOME" "SPARK_CLUSTER_NAME")
verifyVariables $VAR_NAMES  # make sure all variables are set

#export SPARK_EC2_BASE_CONFIG="-k $KEY_PAIR -i $EC2_MUNCHER_IDENTITY_FILE -r eu-west-1 -z eu-west-1c"
export SPARK_EC2_BASE_CONFIG="-k $KEY_PAIR -i $EC2_MUNCHER_IDENTITY_FILE -r eu-west-1 -z eu-west-1b"
