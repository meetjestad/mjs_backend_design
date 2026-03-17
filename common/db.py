import datetime
import hashlib
import typing

import duckdb


class RawMessage(typing.NamedTuple):
    """
    Helper to store the result of a query.

    Since duckdb seems to only return tuples, use the fields_for_select() method to generate a SELECT clause and pass
    the resulting tuple to the RawMessage constructor, to ensure matching field order.

    TODO: Is there no cleaner way to let duckdb figure this out?
    """
    hash: int
    timestamp: datetime.datetime
    src: str
    src_stream: str
    message: str

    @classmethod
    def fields_for_select(cls):
        return ", ".join(cls._fields)

    @classmethod
    def calc_hash(cls, message: str) -> int:
        hash = hashlib.sha256(message.strip().encode())
        # Truncate to 16 bytes and convert to an integer to fit UHUGEINT type.
        # This saves significant space in the database.
        return int.from_bytes(hash.digest()[:16], byteorder='big', signed=False)

    @property
    def hex_hash(self):
        return self.hash.to_bytes(length=16, byteorder='big', signed=False).hex()


def init(database_path):
    con = duckdb.connect(database_path, config={'storage_compatibility_version': 'latest'})

    con.execute("""
    CREATE TABLE IF NOT EXISTS rawmessage (
        hash UHUGEINT PRIMARY KEY,
        timestamp TIMESTAMP WITH TIME ZONE,
        src VARCHAR NOT NULL,
        src_stream VARCHAR NOT NULL,
        message JSON NOT NULL USING COMPRESSION 'zstd',
    )""")

    return con


def shutdown(con):
    con.close()
