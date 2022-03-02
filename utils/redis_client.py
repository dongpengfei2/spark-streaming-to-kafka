import sys
# import redis
from rediscluster import RedisCluster

sys.path.append(".")

cluster_nodes = [
    {'host': '127.0.0.1', 'port': '6080'},
    {'host': '127.0.0.1', 'port': '6081'},
    {'host': '127.0.0.1', 'port': '6082'}
]

cluster_password = "123456"

host = "localhost"


class RedisClient:
    def __init__(self):
        self.conn = RedisCluster(startup_nodes=cluster_nodes, password=cluster_password)
        # self.conn = redis.Redis(host=host)

    def hget(self, name, key):
        return self.conn.hget(name, key)

    def hgetall(self, name):
        return self.conn.hgetall(name)

    def hmset(self, name, mapping):
        self.conn.hmset(name, mapping)

    def close(self):
        self.conn.close()


