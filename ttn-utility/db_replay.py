import datetime
import logging

from iso8601 import parse_date

import common.db
import redis_helpers


def add_arguments(parser):
    parser.add_argument(
        '--start-date',
        required=True,
        help='Start date (inclusive, ISO 8601 format)'
    )
    parser.add_argument(
        '--end-date',
        required=True,
        help='End date (exclusive, ISO 8601 format)'
    )
    parser.add_argument(
        '--stream',
        default='saved.ttn.meet-je-stad',
        help='Redis stream to publish to (default: saved.ttn.meet-je-stad)'
    )


def replay_from_db(redis_server, db_con, args):
    """Replay messages from database to Redis stream within date range"""

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    logging.info("Starting database replay from %s to %s", start_date, end_date)

    # Query messages within the date range
    total_messages, = db_con.execute("""
        SELECT COUNT(*) FROM rawmessage
        WHERE timestamp >= ? AND timestamp < ?
    """, [start_date, end_date]).fetchone()

    if total_messages == 0:
        logging.info("No messages found in the specified date range")
        return

    logging.info("Found %d messages to replay", total_messages)

    period_from = start_date
    interval = datetime.timedelta(days=1)

    # Work around https://github.com/duckdb/duckdb/issues/16166 (full results
    # loaded in memory when using order by) by doing one query per day.
    publish_count = 0
    while period_from < end_date:
        period_to = min(period_from + interval, end_date)

        publish_count += replay_period(redis_server, db_con, args, period_from, period_to)
        logging.info("Replayed %d/%d messages", publish_count, total_messages)

        period_from = period_to

    logging.info("Database replay completed. Replayed %d messages to stream %s", publish_count, args.stream)


def replay_period(redis_server, db_con, args, period_from, period_to):
    messages = db_con.execute(f"""
        SELECT {common.db.RawMessage.fields_for_select()} FROM rawmessage
        WHERE timestamp >= ? AND timestamp < ?
        ORDER BY timestamp
    """, [period_from, period_to])

    publish_count = 0
    while True:
        row = messages.fetchone()
        if row is None:
            break

        msg = common.db.RawMessage(*row)

        try:
            message_data = {
                "src": msg.src,
                "src_id": msg.hex_hash,
                "src_stream": msg.src_stream,
                "received_from_src": msg.timestamp.isoformat(),
                "raw": msg.message,
            }

            redis_helpers.publish(redis_server, args.stream, message_data)

            publish_count += 1

        except Exception as e:
            logging.error("Error replaying message with hash %s: %s", msg.hex_hash, e)
            continue

    return publish_count
