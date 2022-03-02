import sys

sys.path.append(".")

from collections import defaultdict
from utils.redis_client import RedisClient
from utils.offset_manage import OffsetManage
from kafka import TopicPartition, KafkaConsumer


class OffsetMonitor:
    def __init__(self, kafka_consumer=None):
        self.kafka_consumer = kafka_consumer

    def get_last_offset(self, t=None):
        partitions = self.kafka_consumer.partitions_for_topic(t)
        topic_partition = [TopicPartition(t, p) for p in partitions]
        topic_partition_offset = self.kafka_consumer.end_offsets(topic_partition)
        return {key.partition: value for key, value in topic_partition_offset.items()}


if __name__ == '__main__':
    cluster_name = sys.argv[1]
    topic = sys.argv[2]
    group_id = sys.argv[3]

    cluster_dict = {"gpskafka": "127.0.0.1:9092",
                    "testkafka": "192.168.10.101:9092"}

    bootstrap_servers = cluster_dict[cluster_name]

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    offset_monitor = OffsetMonitor(consumer)
    topic_offset_dict = offset_monitor.get_last_offset(topic)

    redis_conn = RedisClient()
    offset_manage = OffsetManage(redis_conn)
    partition_offset = offset_manage.get_last_offset(topic=topic, group_id=group_id)

    partition_offset_dict = defaultdict(int)

    for partition in partition_offset:
        partition_offset_dict[int(partition)] = int(partition_offset[partition])

    topic_offset_lag = {key: value - partition_offset_dict[key] for key, value in topic_offset_dict.items()}

    for k in topic_offset_dict:
        print("partition : %s, log_size : %s, consumer_offset : %s, lag : %s" %
              (k, topic_offset_dict[k], partition_offset_dict[k], topic_offset_lag[k]))
