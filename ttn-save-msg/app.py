#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import logging
import os
import signal
import sys
from urllib.parse import urlparse

import redis
from iso8601 import parse_date

import common.db

database_path = os.environ["MSG_DATABASE_PATH"]
redis_url = urlparse(os.environ["REDIS_URL"])
redis_stream_in = os.environ["REDIS_STREAM_IN"]
redis_stream_out = os.environ["REDIS_STREAM_OUT"]
redis_consumer_group = os.environ["REDIS_CONSUMER_GROUP"]

try:
    redis_maxlen = int(os.environ["REDIS_MAXLEN"])
except KeyError:
    redis_maxlen = None

db_con = common.db.init(database_path)

publish_count = 0

def delete_if_exists(entity, **kwargs):
    # This runs a DELETE query without creating an instance. This bypasses the
    # cache, which could be problematic if the instance would already have been
    # loaded into the cache, but in practice there were actually problems with
    # calling delete() on an instance not deleting it from the cache (so a
    # subsequent insert would fail).
    num = entity.select().where(**kwargs).delete(bulk=True)
    if num:
        logging.info("Deleted previous %s %s", entity.__name__, kwargs)


def process_message(redis_server, entry_id, message):
    global publish_count
    ttn_msg = message['msg']
    src = message['src']
    topic = message['topic']
    timestamp = parse_date(message['timestamp'])

    # First thing, secure the message in the rawest form
    raw_msg = common.db.RawMessage(
        hash=common.db.RawMessage.calc_hash(ttn_msg),
        timestamp=timestamp,
        src=src,
        src_stream=topic,
        message=ttn_msg,
    )
    db_con.values(list(raw_msg)).insert_into('rawmessage')
    logging.debug("Saved message with id %s", raw_msg.hex_hash)

    # Work around https://github.com/redis/redis/issues/14656
    approximate = True
    publish_count += 1
    if publish_count % redis_maxlen == 0:
        approximate = False

    try:
        redis_server.xadd(
            redis_stream_out,
            {
                "db_id": raw_msg.hash,
                "src": raw_msg.src,
                "src_stream": raw_msg.src_stream,
                "received_from_src": raw_msg.timestamp.isoformat(),
                "raw": raw_msg.message,
            },
            # This trims the stream to the given length, but only
            # removes messages that were acked by all consumer
            # groups.
            maxlen=redis_maxlen,
            approximate=approximate,
            ref_policy="ACKED",
        )
        logging.debug("Forwarded message to stream %s", redis_stream_out)
    # pylint: disable=broad-except
    except Exception as ex:
        # TODO: Signal somewhere
        logging.exception("Error processing packet: %s", ex)
        return


def main():
    def terminate(sig, *args):
        print(f"Received signal {sig}, terminating", flush=True)
        sys.exit(0)
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)

    logging.basicConfig(level=logging.DEBUG)

    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0), decode_responses=True,
    )

    # We user a consumer group, not with the intention of having
    # multiple consumers (since we need messages to be in-order
    # usually), but since that allows explicit acking and deleting only
    # acked messages.
    if redis_server.exists(redis_stream_in):
        groups = redis_server.xinfo_groups(name=redis_stream_in)
    else:
        logging.info(f"Stream {redis_stream_in} does not exist yet, will be created along with consumer group")
        groups = []
    if not [g for g in groups if g['name'] in redis_consumer_group]:
        logging.info(f"Creating consumer group {redis_consumer_group} for stream {redis_stream_in}")
        redis_server.xgroup_create(name=redis_stream_in, groupname=redis_consumer_group, id="0", mkstream=True)

    # Since we expect only one consumer, use the group name as the consumer name
    redis_consumer_name = redis_consumer_group
    logging.info(f"Using consumer {redis_consumer_name} in group {redis_consumer_group} for stream {redis_stream_in}")

    while True:
        # The special > id means "messages not seen by any consumer yet
        message_from = '>'
        for stream_name, messages in redis_server.xreadgroup(
            groupname=redis_consumer_group,
            consumername=redis_consumer_name,
            block=60 * 1000,
            streams={redis_stream_in: message_from},
        ):
            for entry_id, message in messages:
                logging.debug("Received message on stream %s: %s", stream_name, message)
                try:
                    process_message(redis_server, entry_id, message)
                    redis_server.xack(stream_name, redis_consumer_group, entry_id)
                # pylint: disable=broad-except
                except Exception as ex:
                    # TODO: Any messages not acked linger in the stream
                    # forever. We should report these errors and have a
                    # way to reprocess pending messages after the
                    # underlying error was fixed?
                    logging.exception("Error processing message: %s", ex)


main()

# vim: set sw=4 sts=4 expandtab:
