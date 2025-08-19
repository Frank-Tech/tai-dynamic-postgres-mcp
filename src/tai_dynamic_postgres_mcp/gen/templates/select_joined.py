from typing import List, Optional, Type, Dict

from psycopg import sql
from psycopg.rows import dict_row

from tai_dynamic_postgres_mcp.database.connection import cursor
from tai_dynamic_postgres_mcp.gen.filters.builder import build_where_clause
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter
from tai_dynamic_postgres_mcp.gen.order.builder import build_order_by_clause
from tai_dynamic_postgres_mcp.gen.order.models import OrderByItem


async def select_joined_tmpl(
        select_clause: str,
        from_clause: str,
        where: Optional[WhereFilter] = None,
        column_map: Optional[Dict[str, str]] = None,
        order_by: Optional[List[OrderByItem]] = None,
        limit: Optional[int] = None,
        model: Optional[Type] = None
) -> List:
    where_clause, where_params = build_where_clause(where, column_map=column_map)
    params = where_params[:]

    query = sql.SQL(select_clause + " " + from_clause)

    if where_clause:
        query += sql.SQL(" WHERE ") + sql.SQL(where_clause)

    order_by_clause, order_params = build_order_by_clause(order_by, column_map=column_map)
    if order_by_clause:
        query += sql.SQL(order_by_clause)
    params.extend(order_params)

    if limit is not None:
        query += sql.SQL(" LIMIT %s")
        params.append(limit)

    async with cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        rows = await cur.fetchall()
        await cur.connection.commit()

    if model:
        return [model(**row) for row in rows]
    return [dict(row) for row in rows]
