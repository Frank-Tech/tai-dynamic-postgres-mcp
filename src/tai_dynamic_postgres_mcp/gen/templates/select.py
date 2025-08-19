from typing import List, Optional, Type

from psycopg import sql
from psycopg.rows import dict_row

from tai_dynamic_postgres_mcp.database.connection import cursor
from tai_dynamic_postgres_mcp.gen.filters.builder import build_where_clause
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter
from tai_dynamic_postgres_mcp.gen.order.builder import build_order_by_clause
from tai_dynamic_postgres_mcp.gen.order.models import OrderByItem

_SELECT_SQL_TEMPLATE = "SELECT * FROM {table}"


async def select_tmpl(
        table: str,
        where: Optional[WhereFilter] = None,
        order_by: Optional[List[OrderByItem]] = None,
        limit: Optional[int] = None,
        model: Optional[Type] = None
) -> List:
    where_clause, where_params = build_where_clause(where)
    params = where_params[:]

    query = sql.SQL(_SELECT_SQL_TEMPLATE).format(
        table=sql.Identifier(*table.split('.'))
    )

    if where_clause:
        query += sql.SQL(" WHERE ") + sql.SQL(where_clause)

    order_by_clause, order_params = build_order_by_clause(order_by)
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
