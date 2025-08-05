from typing import List, Optional, Tuple

from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen
from tai_dynamic_postgres_mcp.gen.schema.schema_parser import parse_schema, sql_columns_to_pydantic_model

_FUNC_PREFIX = "select_joined"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import Optional, List
from pydantic import BaseModel
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.gen.templates.select_joined import select_joined_tmpl
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter

"""

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(where: Optional[WhereFilter] = None) -> List[{model_name}]:
    """
    Selects rows from joined tables: {tables_str}.

    Parameters:
        where: Optional filters to apply using `WhereFilter` on aliased columns (table_column).

    Returns:
        List of `{model_name}` objects from the joined tables.
    """

    return await select_joined_tmpl("{select_clause}", "{from_clause}", where, {column_map_repr}, model={model_name})
'''


class JoinGen(BaseGen):
    def __init__(self, join_groups: List[List[str]], ignore_columns: Optional[List[str]] = None):
        super().__init__(_FUNC_PREFIX, _IMPORTS, _TOOL_TEMPLATE, ignore_columns)
        self.join_groups = join_groups

    def generate_tool(
            self,
            table: str,
            columns: List[tuple],
    ) -> tuple[str, str]:
        raise NotImplementedError("Use generate_join_tool for joins")

    def generate_tools(self, schema: str) -> List[str]:
        tables, fks = parse_schema(schema)
        chunks = [self.imports]
        for group in self.join_groups:
            model_code, tool_code = self.generate_join_tool(group, tables, fks)
            chunks.append(model_code)
            chunks.append(tool_code)
        return chunks

    def find_join_condition(self, table_a: str, table_b: str, fks: List[Tuple[str, str, str, str]]) -> Optional[str]:
        for fk_table, fk_col, ref_table, ref_col in fks:
            if fk_table == table_a and ref_table == table_b:
                return f"{table_a}.{fk_col} = {table_b}.{ref_col}"
            if fk_table == table_b and ref_table == table_a:
                return f"{table_b}.{fk_col} = {table_a}.{ref_col}"
        return None

    def generate_join_tool(
            self,
            group: List[str],
            tables: dict[str, List[Tuple[str, str]]],
            fks: List[Tuple[str, str, str, str]]
    ) -> Tuple[str, str]:
        if len(group) < 2:
            raise ValueError("Join group must have at least two tables.")

        # Build join clauses in the order provided, joining each to a previous table
        joins = []
        current_tables = [group[0]]
        for t in group[1:]:
            cond = None
            for prev in current_tables:
                cond = self.find_join_condition(prev, t, fks)
                if cond:
                    break
            if not cond:
                raise ValueError(f"No foreign key relationship found to join {t} with any of {current_tables}")
            joins.append(f"LEFT JOIN {t} ON {cond}")
            current_tables.append(t)

        from_clause = f"FROM {group[0]} {' '.join(joins)}"

        # Collect columns with aliases and map
        column_map = {}
        select_parts = []
        model_columns = []
        base_table = group[0]
        for t in group:
            if t not in tables:
                raise ValueError(f"Table {t} not found in schema.")
            table_name = t.split('.')[-1]
            for col, typ in tables[t]:
                if col in self.ignore_columns:
                    continue
                alias = f"{table_name}_{col}"
                qualified = f"{t}.{col}"
                column_map[alias] = qualified
                select_parts.append(f"{qualified} AS {alias}")
                if t != base_table and not typ.startswith('Optional['):
                    typ = f"Optional[{typ}]"
                model_columns.append((alias, typ))

        select_clause = "SELECT " + ", ".join(select_parts)

        schema_name = group[0].split('.')[0]
        table_names = "_".join([g.split('.')[-1] for g in group])
        joined_group = f"{schema_name}_{table_names}"
        model_name, model_code = sql_columns_to_pydantic_model(self.prefix, joined_group, model_columns)

        tool_code = self.template.format(
            func_name=self.func_name(joined_group),
            model_name=model_name,
            tables_str=", ".join(group),
            select_clause=select_clause,
            from_clause=from_clause,
            column_map_repr=repr(column_map)
        )

        return model_code, tool_code
