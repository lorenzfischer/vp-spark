#!/bin/bash

MUNCHER_JAR=target/scala-2.10/vp-spark-assembly-1.0.0-SNAPSHOT.jar
MUNCHER_INPUT="../db-dump.tar.gz"
MUNCHER_OUTPUT="munch-output"  # the resulting file will be called $MUNCHER_OUTPUT.tar.gz
MUNCHER_OUTPUT_TAR_FILE="$MUNCHER_OUTPUT.tar.gz"