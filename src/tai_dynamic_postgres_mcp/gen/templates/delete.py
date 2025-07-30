from typing import Optional

from psycopg import sql

from tai_dynamic_postgres_mcp.database.connection import cursor
from tai_dynamic_postgres_mcp.gen.filters.builder import build_where_clause
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter

_DELETE_SQL_TEMPLATE = "DELETE FROM {table}"


async def delete_tmpl(
        table: str,
        where: Optional[WhereFilter] = None,
) -> int:
    where_clause, params = build_where_clause(where)

    query = sql.SQL(_DELETE_SQL_TEMPLATE).format(
        table=sql.Identifier(*table.split('.'))
    )

    if where_clause:
        query += sql.SQL(" WHERE ") + sql.SQL(where_clause)

    async with cursor() as cur:
        await cur.execute(query, params)
        await cur.connection.commit()
        return cur.rowcount
