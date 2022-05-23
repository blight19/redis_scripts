#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2022/4/26
import time
import redis
from conf import *
from concurrent.futures import ThreadPoolExecutor
from typing import List
from rwlock import ReadWriteLock

pool = ThreadPoolExecutor(max_workers=2)
conn = redis.Redis(host=host, username=username, password=password)
if conn.client_tracking_on(conn.client_id(), bcast=True,prefix="key") == b'OK':
    print('跟踪成功')
else:
    print("发生错误")

pubsub = conn.pubsub()
pubsub.subscribe("_redis_:invalidate")
lock = ReadWriteLock()
local_cache = {}


def get(key):
    if key in local_cache:
        lock.acquire_read()
        value = local_cache[key]
        lock.release_read()
    else:
        value = conn.get(key)
        lock.acquire_write()
        local_cache[key] = value
        lock.release_write()
    return value


def get_keys():
    while True:
        key = "key:1"
        print(f"获取{key}:{get(key)}")
        time.sleep(1)


def watch_keys():
    for x in pubsub.listen():
        if isinstance(x['data'], List):
            print("收到消息")
            for key in x['data']:
                lock.acquire_write()
                decode_key = key.decode()
                if decode_key in local_cache:
                    del local_cache[decode_key]
                lock.release_write()
                print(f"删除了key:{key}")


pool.submit(get_keys)
pool.submit(watch_keys)
