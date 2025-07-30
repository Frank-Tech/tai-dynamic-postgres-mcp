from typing import Optional

from psycopg import sql

from tai_dynamic_postgres_mcp.database.connection import cursor
from tai_dynamic_postgres_mcp.gen.filters.builder import build_where_clause
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter


async def update_tmpl(
        table: str,
        data,
        where: Optional[WhereFilter] = None
) -> int:
    update_fields = {
        k: v for k, v in data.model_dump(exclude_none=True).items()
    }
    if not update_fields:
        return 0

    set_clauses = [sql.SQL(f"{k} = %s") for k in update_fields]
    set_values = list(update_fields.values())

    where_clause, where_params = build_where_clause(where)

    query = sql.SQL("UPDATE {table} SET ").format(
        table=sql.Identifier(*table.split('.'))
    )
    query += sql.SQL(', ').join(set_clauses)

    if where_clause:
        query += sql.SQL(" WHERE ") + sql.SQL(where_clause)

    async with cursor() as cur:
        await cur.execute(query, set_values + where_params)
        await cur.connection.commit()
        return cur.rowcount
