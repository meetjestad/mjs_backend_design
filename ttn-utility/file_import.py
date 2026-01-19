import csv
import json
import logging
from compression import zstd

from iso8601 import parse_date

import redis_helpers


def import_from_file(redis_server, path, stream_name):
    """Import messages from zstd-encoded TSV file to Redis stream"""
    logging.info("Starting import from file: %s", path)

    with zstd.open(path, 'rt', newline='') as file:
        reader = csv.DictReader(file, delimiter='\t', quoting=csv.QUOTE_NONE)

        required_fields = ['timestamp', 'source', 'message']
        if not all(field in reader.fieldnames for field in required_fields):
            logging.error("Missing required fields in header: %s. Required: %s", reader.fieldnames, required_fields)
            return

        publish_count = 0
        for row in reader:
            try:
                timestamp_str = row['timestamp']
                source = row['source']
                message_json = row['message']

                timestamp = parse_date(timestamp_str)

                # Validate JSON message before publishing
                json.loads(message_json)

                message_data = {
                    "msg": message_json,
                    "timestamp": timestamp.isoformat(),
                    # TODO: Use actual topic here, and use correct format depending on ttn version
                    "topic": "v3/meet-je-stad@ttn/devices/meetstation-xxx/up",
                    "src": source,
                }

                redis_helpers.publish(redis_server, stream_name, message_data)

                publish_count += 1
                if publish_count % 1000 == 0:
                    logging.info("Published %d messages", publish_count)

            except Exception as e:
                logging.error("Error processing row: %s", e)
                continue

    logging.info("Import completed. Published %d messages to stream %s", publish_count, stream_name)
