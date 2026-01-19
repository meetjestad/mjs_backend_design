#!/usr/bin/env python3

import argparse
import logging
import os
import signal
import sys
from urllib.parse import urlparse

import redis
from iso8601 import parse_date

import db
from file_import import import_from_file
from db_replay import replay_from_db


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


def cmd_import_from_file(redis_server, args):
    """Import messages from zstd TSV file to Redis stream."""
    try:
        for file in args.files:
            import_from_file(redis_server, file, args.stream)
    except Exception as e:
        logging.exception("File import failed", exc_info=e)
        sys.exit(1)


def cmd_replay_from_db(redis_server, args):
    """Replay messages from database to Redis stream."""
    try:
        start_dt = parse_date(args.start_date)
        end_dt = parse_date(args.end_date)
        replay_from_db(redis_server, start_dt, end_dt, args.stream)
        logging.info("Database replay completed successfully")
    except Exception as e:
        logging.error("Database replay failed: %s", exc_info=e)
        sys.exit(1)


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

    # import-from-file command
    import_parser = subparsers.add_parser(
        'import-from-file',
        help='Import messages from zstd TSV file to Redis stream'
    )
    import_parser.add_argument(
        '--stream',
        default='ttn.meet-je-stad',
        help='Redis stream to publish to (default: ttn.meet-je-stad)'
    )
    import_parser.add_argument(
        'files',
        nargs='+',
        help='TSV files to import (zstd-encoded)'
    )

    # replay-from-db command
    replay_parser = subparsers.add_parser(
        'replay-from-db',
        help='Replay messages from database to Redis stream'
    )
    replay_parser.add_argument(
        '--start-date',
        required=True,
        help='Start date (inclusive, ISO 8601 format)'
    )
    replay_parser.add_argument(
        '--end-date',
        required=True,
        help='End date (exclusive, ISO 8601 format)'
    )
    replay_parser.add_argument(
        '--stream',
        default='saved.ttn.meet-je-stad',
        help='Redis stream to publish to (default: saved.ttn.meet-je-stad)'
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Setup shared context
    redis_server = setup_redis_connection()

    # Execute command
    if args.command == 'import-from-file':
        cmd_import_from_file(redis_server, args)
    elif args.command == 'replay-from-db':
        database_url = urlparse(os.environ["DATABASE_URL"])
        db.init(database_url)

        cmd_replay_from_db(redis_server, args)


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, _terminate)
    signal.signal(signal.SIGINT, _terminate)
    main()


# vim: set sw=4 sts=4 expandtab:
