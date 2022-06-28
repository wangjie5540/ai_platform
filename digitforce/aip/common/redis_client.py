import logging
import time

import redis

from digitforce.aip.common.config.bigdata_config import REDIS_HOST, REDIS_PORT


class RedisClient(object):

    def __init__(self, host, port=6379, db=0, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.redis_obj = None
        self.disconnect_time = -1

    def get_redis_obj(self):
        if self.redis_obj is None:
            self.redis_obj = redis.Redis(host=self.host, port=self.port,
                                         password=self.password, db=self.db,
                                         max_connections=10,
                                         retry_on_timeout=True)
            self.disconnect_time = time.time()
        # crontab disconnect all connections
        if self.redis_obj:
            if time.time() - self.disconnect_time > 60:
                self.redis_obj.connection_pool.disconnect()
                self.disconnect_time = time.time()
        return self.redis_obj

    def update_redis_string(self, key: str, value: str, expire_time=None):
        return self.get_redis_obj().set(key, value, ex=expire_time)

    def get_redis_string(self, key: str):
        return self.get_redis_obj().get(key)

    def update_redis_list(self, key: str, values: list, expire_time=30 * 24 * 3600):
        # pipeline = self.get_redis_obj().pipeline()
        # pipeline.delete(key)
        # pipeline.rpush(key, *values)
        # if expire_time:
        #     pipeline.expire(key, int(expire_time))
        # pipeline.execute()
        self.get_redis_obj().delete(key)
        self.get_redis_obj().rpush(key, *values)
        self.get_redis_obj().expire(key, int(expire_time))

    def get_redis_list(self, key: str):
        pipeline = self.get_redis_obj().pipeline()
        pipeline.lrange(key, 0, -1)
        values = pipeline.execute()
        result = []
        if len(values) >= 0:
            result += values[0]
        return result

    def update_redis_set(self, key: str, value_set: set, expire_time=24 * 3600):
        pipeline = self.get_redis_obj().pipeline()
        pipeline.delete(key)
        pipeline.sadd(key, *value_set)
        if expire_time:
            pipeline.expire(key, int(expire_time))
        pipeline.execute()

    def in_redis_set(self, key: str, value):
        pipeline = self.get_redis_obj().pipeline()
        pipeline.sismember(key, value)
        values = pipeline.execute()
        return values[0]

    def get_redis_sorted_set(self, key: str, start=0, end=-1):
        pipeline = self.get_redis_obj().pipeline()
        pipeline.zrange(key, start, end, desc=True)
        values = pipeline.execute()
        result = []
        for value in values:
            for _ in value:
                result.append(_)
        return result

    def add_redis_sorted_set(self, key: str, value: str, score: float, expire_time=24 * 3600):
        pipeline = self.get_redis_obj().pipeline()
        pipeline.zadd(key, {value: score}, nx=True)
        if expire_time:
            pipeline.expire(key, int(expire_time))
        pipeline.execute()

    def get_keys(self, key_fmt: str, max_result_size=1000000):
        result = []
        cursor_number = 0
        while True:
            if len(result) > max_result_size > 0:
                logging.warning(f"[WARNING] find too many keys ... not all keys will be return ... "
                                f"use bigger max_result_size")
                break
            cursor_number, keys = self.get_redis_obj().scan(cursor_number, key_fmt, 1000)
            result += keys
            if cursor_number == 0:
                break
        result = [_.decode() for _ in result]
        return result

    def expire_keys(self, keys, expire_time=1):
        pipeline = self.get_redis_obj().pipeline()
        for key in keys:
            pipeline.expire(key, int(expire_time))
        pipeline.execute()

    def get_key_expire_time(self, key):
        return self.get_redis_obj().ttl(key)

    def set_expire_time(self, key, expire_time):
        self.get_redis_obj().expire(key, expire_time)


df_redis_cli = RedisClient(host=REDIS_HOST, port=REDIS_PORT)
if __name__ == '__main__':
    df_redis_cli = RedisClient("172.21.32.143")
    print(df_redis_cli.get_redis_string("test"))
