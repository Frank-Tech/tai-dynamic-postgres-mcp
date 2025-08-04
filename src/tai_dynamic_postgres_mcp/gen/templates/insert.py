from typing import List, Tuple

from psycopg import sql

from tai_dynamic_postgres_mcp.database.connection import cursor

_INSERT_SQL_TEMPLATE = "INSERT INTO {table} ({columns}) VALUES {values} {conflict_clause} RETURNING id"


async def insert_tmpl(
        table: str,
        columns: List[str],
        values: List[Tuple],
        raise_on_conflict: bool = True
) -> List[int]:
    if not values:
        return []

    flat_values = [item for row in values for item in row]

    values_placeholders = sql.SQL(', ').join(
        sql.SQL('({})').format(sql.SQL(', ').join(sql.Placeholder() for _ in columns)) for _ in values
    )

    conflict_clause = sql.SQL("") if raise_on_conflict else sql.SQL("ON CONFLICT DO NOTHING")

    query = sql.SQL(_INSERT_SQL_TEMPLATE).format(
        table=sql.Identifier(*table.split('.')),
        columns=sql.SQL(', ').join(sql.Identifier(col) for col in columns),
        values=values_placeholders,
        conflict_clause=conflict_clause
    )

    async with cursor() as cur:
        await cur.execute(query, flat_values)
        result = await cur.fetchall()
        await cur.connection.commit()
        return [row[0] for row in result]
