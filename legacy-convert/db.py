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
