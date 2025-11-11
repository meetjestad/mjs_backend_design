#!/usr/bin/env python3
# vim:fileencoding=utf8
# pylint: disable=missing-docstring
import base64
import json
import logging
import os
from urllib.parse import urlparse

import redis
from iso8601 import parse_date
from pony import orm

import db

database_url = urlparse(os.environ["DATABASE_URL"])
redis_url = urlparse(os.environ["REDIS_URL"])
redis_stream_in = os.environ["REDIS_STREAM_IN"]
redis_stream_out = os.environ["REDIS_STREAM_OUT"]
try:
    redis_maxlen = int(os.environ["REDIS_MAXLEN"])
except KeyError:
    redis_maxlen = None

db.init(database_url)


def delete_if_exists(entity, **kwargs):
    # This runs a DELETE query without creating an instance. This bypasses the
    # cache, which could be problematic if the instance would already have been
    # loaded into the cache, but in practice there were actually problems with
    # calling delete() on an instance not deleting it from the cache (so a
    # subsequent insert would fail).
    num = entity.select().where(**kwargs).delete(bulk=True)
    if num:
        logging.info("Deleted previous %s %s", entity.__name__, kwargs)


@orm.db_session
def process_message(redis_server, entry_id, message):
    ttn_msg = message['msg']
    src = message['src']
    topic = message['topic']
    timestamp = parse_date(message['timestamp'])

    # TODO: Use session_key_id and fcnt to generate an id
    # First thing, secure the message in the rawest form
    delete_if_exists(db.RawMessage, src=src, src_id=entry_id)
    raw_msg = db.RawMessage(
        src=src,
        # TTN does not assign ids, so use the id assigned by redis then
        src_id=entry_id,
        src_stream=topic,
        received_from_src=timestamp,
        raw=ttn_msg,
    )
    orm.commit()

    try:
        redis_server.xadd(
            redis_stream_out,
            {
                "db_id": raw_msg.id,
                "src": raw_msg.src,
                "src_id": raw_msg.src_id,
                "src_stream": raw_msg.src_stream,
                "received_from_src": raw_msg.received_from_src.isoformat(),
                "raw": raw_msg.raw,
            },
            # This trims the stream to the given length, but only
            # removes messages that were acked by all consumer
            # groups.
            maxlen=redis_maxlen,
            approximate=True,
            ref_policy="ACKED",
        )
    # pylint: disable=broad-except
    except Exception as ex:
        # TODO: Signal somewhere
        logging.exception("Error processing packet: %s", ex)
        return


def main():
    logging.basicConfig(level=logging.DEBUG)

    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    redis_server = redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0), decode_responses=True,
    )

    # TODO: Consumer group
    messages_from = "0"
    while True:
        for stream_name, messages in redis_server.xread(
                {redis_stream_in: messages_from}, block=60 * 1000
        ):
            for entry_id, message in messages:
                messages_from = entry_id
                try:
                    process_message(redis_server, entry_id, message)
                # pylint: disable=broad-except
                except Exception as ex:
                    logging.exception("Error processing message: %s", ex)


main()

# vim: set sw=4 sts=4 expandtab:
