#!/usr/bin/env python3

import argparse
import glob
import logging
import os
import signal
import sys
from urllib.parse import urlparse

import redis

import db
import file_import
import db_replay


class SharedContext:
    """Shared context for CLI commands."""
    def __init__(self):
        self.redis_server = None


def setup_redis_connection():
    """Setup Redis connection and return SharedContext."""
    redis_url = urlparse(os.environ["REDIS_URL"])
    logging.info(
        "Connecting Redis to {} on port {}".format(redis_url.hostname, redis_url.port)
    )
    return redis.Redis(
        host=redis_url.hostname, port=redis_url.port, db=int(redis_url.path[1:] or 0), decode_responses=True,
    )


def _terminate(sig, *args):
    print(f"Received signal {sig}, terminating", flush=True)
    sys.exit(0)


def main():
    """Main CLI entry point."""
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='TTN Utility for importing and replaying messages.',
        prog='ttn-utility'
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    import_parser = subparsers.add_parser(
        'import-from-file',
        help='Import messages from zstd TSV file to Redis stream'
    )
    file_import.add_arguments(import_parser)

    replay_parser = subparsers.add_parser(
        'replay-from-db',
        help='Replay messages from database to Redis stream'
    )
    db_replay.add_arguments(replay_parser)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Setup shared context
    redis_server = setup_redis_connection()

    database_path = os.environ["MSG_DATABASE_PATH"]
    db_con = db.init(database_path)

    # Execute command
    if args.command == 'import-from-file':
        file_import.import_from_file(redis_server, db_con, args)
    elif args.command == 'replay-from-db':
        db_replay.replay_from_db(redis_server, db_con, args)

    db.shutdown(db_con)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    main()


# vim: set sw=4 sts=4 expandtab:
