import sys

sys.path.append(".")

profile_key = "data_kafka_offset_%s_%s"


class OffsetManage:

    def __init__(self, redis_client=None):
        self.redis_client = redis_client

    def get_last_offset(self, topic=None, group_id=None):
        redis_key = profile_key % (topic, group_id)
        return self.redis_client.hgetall(redis_key)

    def set_last_offset(self, topic=None, group_id=None, partition_dict=None):
        if partition_dict:
            redis_key = profile_key % (topic, group_id)
            self.redis_client.hmset(redis_key, partition_dict)
