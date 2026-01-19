import logging

from pony import orm

import db
import redis_helpers


@orm.db_session
def replay_from_db(redis_server, start_date, end_date, stream_name):
    """Replay messages from database to Redis stream within date range"""
    logging.info("Starting database replay from %s to %s", start_date, end_date)

    # Query messages within the date range
    messages = db.RawMessage.select(
        lambda rm: rm.received_from_src >= start_date and rm.received_from_src < end_date
    ).order_by(db.RawMessage.received_from_src)

    total_messages = messages.count()
    logging.info("Found %d messages to replay", total_messages)

    if total_messages == 0:
        logging.info("No messages found in the specified date range")
        return

    publish_count = 0
    for raw_msg in messages:
        try:
            message_data = {
                "db_id": raw_msg.id,
                "src": raw_msg.src,
                "src_id": raw_msg.src_id or "",
                "src_stream": raw_msg.src_stream or "",
                "received_from_src": raw_msg.received_from_src.isoformat(),
                "raw": raw_msg.raw,
            }

            redis_helpers.publish(redis_server, stream_name, message_data)

            publish_count += 1

            if publish_count % 1000 == 0:
                logging.info("Replayed %d/%d messages", publish_count, total_messages)

        except Exception as e:
            logging.error("Error replaying message with db_id %s: %s", raw_msg.id, e)
            continue

    logging.info("Database replay completed. Replayed %d messages to stream %s", publish_count, stream_name)
