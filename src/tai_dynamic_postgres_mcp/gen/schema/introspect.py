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

_FK_QUERY = """
            SELECT tc.table_schema,
                   tc.table_name,
                   kcu.column_name,
                   ccu.table_schema AS ref_table_schema,
                   ccu.table_name   AS ref_table_name,
                   ccu.column_name  AS ref_column_name,
                   rc.update_rule,
                   rc.delete_rule
            FROM information_schema.table_constraints AS tc
                     JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                              AND tc.table_schema = kcu.table_schema
                     JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                     JOIN information_schema.referential_constraints AS rc
                          ON rc.constraint_name = tc.constraint_name
                              AND rc.constraint_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'; \
            """


async def generate_schema_ddl() -> str:
    tables: Dict[str, List[str]] = defaultdict(list)

    async with cursor() as cur:
        await cur.execute(_DDL_RAW_QUERY)
        rows = await cur.fetchall()

        for schema, table, column, col_type, notnull in rows:
            nullability = "NOT NULL" if notnull else "NULL"
            tables[f"{schema}.{table}"].append(f"    {column} {col_type} {nullability}")

        await cur.execute(_FK_QUERY)
        fk_rows = await cur.fetchall()

        for tc_schema, tc_table, kcu_column, ref_schema, ref_table, ref_column, update_rule, delete_rule in fk_rows:
            fk_line = f"    FOREIGN KEY ({kcu_column}) REFERENCES {ref_schema}.{ref_table} ({ref_column})"
            if update_rule != 'NO ACTION':
                fk_line += f" ON UPDATE {update_rule}"
            if delete_rule != 'NO ACTION':
                fk_line += f" ON DELETE {delete_rule}"
            tables[f"{tc_schema}.{tc_table}"].append(fk_line)

    ddl_statements = [
        f"CREATE TABLE {table} (\n" + ",\n".join(columns) + "\n);"
        for table, columns in tables.items()
    ]

    return "\n\n".join(ddl_statements)
