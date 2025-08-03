from psycopg.adapt import Loader
from psycopg.types import TypeInfo

TEMPORAL_TYPES = [
    'date',
    'time',
    'timetz',
    'timestamp',
    'timestamptz',
]


class TextLoader(Loader):
    def load(self, data: memoryview) -> str:
        return bytes(data).decode("utf-8")


async def register_temporal_types_as_strings(conn):
    for type_name in TEMPORAL_TYPES:
        tinfo = await TypeInfo.fetch(conn, type_name)
        conn.adapters.register_loader(tinfo.oid, TextLoader)
