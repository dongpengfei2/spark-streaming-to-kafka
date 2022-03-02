#!/usr/bin/env bash

export LANG="en_US.UTF-8"
export PYTHONIOENCODING=utf8

source_brokers="127.0.0.1:9092"
source_topic="dwd_pcr"

target_brokers="127.0.0.1:9092"
target_topic="dws_event"

checkpoint_dir="hdfs:///user/spark_streaming/checkpoint/pcr_report"

spark-submit \
--master yarn \
--queue t1 \
--num-executors 6 \
--driver-memory 2g \
--executor-memory 2g \
--executor-cores 2 \
--conf "spark.driver.extraClassPath=lz4-1.2.0.jar" \
--conf "spark.executor.extraClassPath=lz4-1.2.0.jar" \
--jars jars/lz4-1.2.0.jar,jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar \
--py-files ./spark-streaming-to-kafka.zip \
scripts/main.py $source_brokers $source_topic $target_brokers $target_topic $checkpoint_dir > /dev/null 2>&1 &