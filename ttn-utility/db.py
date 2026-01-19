from datetime import datetime
from pony import orm

# Below, datetime types specify the sql_type explicitly, to ensure timezone
# information is stored along with the timestamps. See also
# https://github.com/ponyorm/pony/issues/434

db = orm.Database()


def init(database_url):
    db.bind(
        provider=database_url.scheme,
        user=database_url.username,
        password=database_url.password,
        host=database_url.hostname,
        port=database_url.port,
        database=database_url.path[1:],
    )

    db.generate_mapping(create_tables=True)
    return db


class RawMessage(db.Entity):
    # Single id primary key to make it easier to refer to these messages
    id = orm.PrimaryKey(int, auto=True)

    # Source type and source-specific id to allow correlating with upstream messages, if any
    src = orm.Required(str)
    src_id = orm.Optional(str)
    src_stream = orm.Optional(str)
    received_from_src = orm.Optional(datetime, sql_type='TIMESTAMP WITH TIME ZONE')

    raw = orm.Optional(str)
