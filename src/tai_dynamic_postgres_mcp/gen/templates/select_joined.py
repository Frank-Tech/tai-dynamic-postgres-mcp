from typing import List, Optional, Type

from psycopg import sql
from psycopg.rows import dict_row

from tai_dynamic_postgres_mcp.database.connection import cursor
from tai_dynamic_postgres_mcp.gen.filters.builder import build_where_clause
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter


async def select_joined_tmpl(
        select_clause: str,
        from_clause: str,
        where: Optional[WhereFilter] = None,
        column_map: Optional[dict[str, str]] = None,
        model: Optional[Type] = None
) -> List:
    where_clause, params = build_where_clause(where, column_map=column_map)

    query = sql.SQL(select_clause + " " + from_clause)

    if where_clause:
        query += sql.SQL(" WHERE ") + sql.SQL(where_clause)

    async with cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()
        await cur.connection.commit()

    if model:
        return [model(**row) for row in rows]
    return [dict(row) for row in rows]
