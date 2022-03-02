from __future__ import print_function
import json
import os
import sys

sys.path.append(".")

from kafka import KafkaProducer
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark import SparkConf, SparkContext

cur_dir = os.path.dirname(os.path.realpath(sys.argv[0]))
sys.path.append(cur_dir + '/../scripts')
sys.path.append(cur_dir + '/../utils')

from scripts.norm_udf import run_cpp_func, mapping_unique_id, send_kafka
from utils.offset_manage import OffsetManage
from utils.redis_client import RedisClient


if __name__ == "__main__":

    source_brokers = sys.argv[1]
    source_topic = sys.argv[2]
    group_id = 'offline'

    target_brokers = sys.argv[3]
    target_topic = sys.argv[4]

    checkpoint_dir = sys.argv[5]

    print("source_brokers : %s" % source_brokers)
    print("source_topic : %s" % source_topic)
    print("target_brokers : %s" % target_brokers)
    print("target_topic : %s" % target_topic)
    print("checkpoint_dir : %s" % checkpoint_dir)

    app_name = 'spark-streaming-kafka[%s]-to-kafka[%s]' % (source_topic, target_topic)

    offsetRanges = []


    def storeOffsetRanges(rdd):
        global offsetRanges
        offsetRanges = rdd.offsetRanges()
        return rdd


    def foreach_partition_func(partitions):
        producer = KafkaProducer(bootstrap_servers=target_brokers)
        partition_conn = RedisClient()
        for message in partitions:
            line = mapping_unique_id(partition_conn, message)
            key = line["unique_id"]
            send_kafka(producer, target_topic, key, line)
        producer.close()
        partition_conn.close()


    def foreach_rdd_func(rdds):
        rdds.foreachPartition(lambda partitions: foreach_partition_func(partitions))
        last_offset = {}
        for o in offsetRanges:
            last_offset[o.partition] = o.untilOffset

        if len(last_offset) > 0:
            rdd_conn = RedisClient()
            rdd_om = OffsetManage(rdd_conn)
            rdd_om.set_last_offset(source_topic, group_id, last_offset)
            rdd_conn.close()


    conf = SparkConf() \
        .set('spark.io.compression.codec', "snappy")

    sc = SparkContext(appName=app_name, conf=conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir(checkpoint_dir)
    ssc = StreamingContext(sc, 10)

    kafkaParams = {"metadata.broker.list": source_brokers, "group.id": group_id, "compression.codec": "snappy",
                   "auto.offset.reset": "largest"}

    fromOffsets = {}
    conn = RedisClient()
    om = OffsetManage(conn)
    offset_dict = om.get_last_offset(source_topic, group_id)
    for partition in offset_dict:
        fromOffsets[TopicAndPartition(topic=source_topic, partition=int(partition))] = long(offset_dict[partition])
    conn.close()

    print("offset_dict : %s" % offset_dict)

    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [source_topic],
                                                        kafkaParams=kafkaParams, fromOffsets=fromOffsets)
    lines_rdd = kafka_streaming_rdd.transform(lambda line: storeOffsetRanges(line)).checkpoint(300) \
        .map(lambda x: json.loads(x[1]))
    schema_rdd = lines_rdd.map(lambda line: run_cpp_func(source_topic, "cpp_schema", line))
    filter_rdd = schema_rdd.filter(lambda line: run_cpp_func(source_topic, "cpp_filter", line))
    normal_rdd = filter_rdd.map(lambda line: run_cpp_func(source_topic, "cpp_table", line))
    normal_rdd.foreachRDD(lambda rdd: foreach_rdd_func(rdd))

    ssc.start()
    ssc.awaitTermination()
