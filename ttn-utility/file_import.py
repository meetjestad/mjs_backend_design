import glob
import json
import logging
import sys
import time
import pathlib

import duckdb

import db


def add_arguments(parser):
    parser.add_argument(
        '--glob',
        action='store_true',
        help='Interpret filenames as glob patterns (useful to glob inside docker instead of in the shell outdide)',
    )
    parser.add_argument(
        '--ignore-conflicts',
        action='store_true',
        help='On message hash id conflicts, keep the existing message and drop the new one',
    )
    parser.add_argument(
        'files',
        nargs='+',
        help='TSV files to import (zstd-encoded)'
    )


def import_from_file(redis_server, db_con, args):
    """Import messages from zstd TSV file to Redis stream."""
    if args.glob:
        files = [file for pattern in args.files for file in sorted(glob.glob(pattern))]
    else:
        files = args.files

    for file in files:
        import_one_file(redis_server, db_con, pathlib.Path(file), args)


def import_one_file(redis_server, db_con, file, args):
    """Import messages from zstd-encoded TSV file to Redis stream"""
    logging.info("%s: Starting import", file.name)
    start = time.monotonic()

    def calc_src_stream(src: str, message: str) -> str:
        msg_obj = json.loads(message)
        if src == "ttn.v3":
            device_id = msg_obj["end_device_ids"]["device_id"]
            app_id = msg_obj["end_device_ids"]["application_ids"]["application_id"]
            return f"v3/{app_id}@ttn/devices/{device_id}/up"

    # Convert hash to string and back to UHUGEINT in the query below to work around https://github.com/duckdb/duckdb-python/issues/330
    # db_con.create_function('calc_hash', db.RawMessage.calc_hash, return_type=duckdb.sqltypes.UHUGEINT)
    db_con.create_function('calc_hash', lambda *args: str(db.RawMessage.calc_hash(*args)), return_type=duckdb.sqltypes.VARCHAR)
    db_con.create_function('calc_src_stream', calc_src_stream)

    try:
        (inserted,) = db_con.execute(f"""
            INSERT INTO rawmessage BY NAME (
                SELECT *, calc_hash(message)::UHUGEINT as hash FROM (
                    SELECT timestamp, source as src, calc_src_stream(source, message) as src_stream, message
                    FROM read_csv(?,
                        delim = '\t',
                        header = true
                    ) LIMIT 1
                )
            ) {"ON CONFLICT DO NOTHING" if args.ignore_conflicts else ""}
        """, [str(file)]).fetchone()
    except duckdb.ConstraintException as e:
        logging.error("%s: %s", file.name, e)
        logging.error("%s: Aborting, entire file is not imported (but previous files, if any, are)", file.name)
        sys.exit(1)
    finally:
        db_con.remove_function('calc_hash')
        db_con.remove_function('calc_src_stream')

    logging.info("%s: Imported %s rows in %s seconds", file.name, inserted, round(time.monotonic() - start))

    return 0
