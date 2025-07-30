from collections import defaultdict
from typing import Dict, List

from tai_dynamic_postgres_mcp.database.connection import cursor

_DDL_RAW_QUERY = """
                 SELECT n.nspname AS schema, 
                    c.relname AS table, 
                    a.attname AS column, 
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                    a.attnotnull AS notnull
                 FROM pg_class c
                     JOIN pg_namespace n
                 ON n.oid = c.relnamespace
                     JOIN pg_attribute a ON a.attrelid = c.oid
                 WHERE c.relkind = 'r'
                   AND a.attnum
                     > 0
                   AND NOT a.attisdropped
                   AND n.nspname NOT IN ('pg_catalog'
                     , 'information_schema')
                 ORDER BY n.nspname, c.relname, a.attnum; \
                 """


async def generate_schema_ddl() -> str:
    tables: Dict[str, List[str]] = defaultdict(list)

    async with cursor() as cur:
        await cur.execute(_DDL_RAW_QUERY)
        rows = await cur.fetchall()

        for schema, table, column, col_type, notnull in rows:
            nullability = "NOT NULL" if notnull else "NULL"
            tables[f"{schema}.{table}"].append(f"    {column} {col_type} {nullability}")

    ddl_statements = [
        f"CREATE TABLE {table} (\n" + ",\n".join(columns) + "\n);"
        for table, columns in tables.items()
    ]

    return "\n\n".join(ddl_statements)
