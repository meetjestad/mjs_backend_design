import logging
import os
import time

import redis

try:
    redis_maxlen = int(os.environ["REDIS_MAXLEN"])
except KeyError:
    redis_maxlen = None

publish_count = 0


def publish(redis_server: redis.Redis, stream_name: str, message_data):
    global publish_count
    # Work around https://github.com/redis/redis/issues/14656
    approximate = True
    publish_count += 1
    if redis_maxlen and publish_count % redis_maxlen == 0:
        approximate = False

    redis_server.xadd(
        stream_name,
        message_data,
        maxlen=redis_maxlen,
        approximate=approximate,
        ref_policy="ACKED",
    )

    # Prevent overloading redis (filling memory), better to just wait
    if not approximate:
        limit = 2 * redis_maxlen
        if redis_server.xlen(stream_name) >= limit:
            logging.info("Redis stream filling up, waiting to add more items...")
            while redis_server.xlen(stream_name) >= limit:
                time.sleep(5)
                redis_server.xtrim(
                    stream_name,
                    maxlen=redis_maxlen,
                    approximate=approximate,
                    ref_policy="ACKED",
                )
