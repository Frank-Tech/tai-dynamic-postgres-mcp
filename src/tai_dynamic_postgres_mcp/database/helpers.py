from typing import List

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


class VectorLoader(Loader):
    def load(self, data: memoryview) -> List[float]:
        s = bytes(data).decode("utf-8")
        if s.startswith('[') and s.endswith(']'):
            values = s[1:-1].split(',')
            return [float(x.strip()) for x in values if x.strip()]
        else:
            raise ValueError(f"Invalid vector format: {s}")


async def register_vector_as_list(conn):
    tinfo = await TypeInfo.fetch(conn, 'vector')
    conn.adapters.register_loader(tinfo.oid, VectorLoader)


async def register_types_loaders(conn):
    await register_temporal_types_as_strings(conn)
    await register_vector_as_list(conn)
