from typing import List, Optional

from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen
from tai_dynamic_postgres_mcp.gen.schema.schema_parser import sql_columns_to_pydantic_model

_FUNC_PREFIX = "insert"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import Optional, List
from pydantic import BaseModel
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.gen.templates.insert import insert_tmpl

"""

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(params: List[{model_name}], raise_on_conflict: bool = True) -> List[int]:
    """
    Inserts multiple rows into the `{table}` table.

    Parameters:
        params: List of `{model_name}` objects in the order: {doc_params}
        raise_on_conflict: If True, raise an error on unique constraint conflict. 
                           If False, conflicting rows will be ignored (ON CONFLICT DO NOTHING).

    Returns:
        List of inserted row IDs from the `{table}` table.
    """

    values = [({args}) for row in params or []]

    return await insert_tmpl("{table}", {col_list}, values, raise_on_conflict)
'''


class InsertGen(BaseGen):
    def __init__(self, ignore_insert_columns: Optional[List[str]] = None):
        super().__init__(_FUNC_PREFIX, _IMPORTS, _TOOL_TEMPLATE, ignore_insert_columns)

    def generate_tool(
            self,
            table: str,
            columns: List[tuple],
    ) -> tuple[str, str]:
        insert_columns = [(col, typ) for col, typ in columns if col not in self.ignore_columns]

        model_name, model_code = sql_columns_to_pydantic_model(self.prefix, table, insert_columns)

        doc_params = ', '.join([col for col, _ in insert_columns])
        args = ', '.join([f"row.{col}" for col, _ in insert_columns])
        col_list = [col for col, _ in insert_columns]
        num_cols = len(insert_columns)

        tool_code = self.template.format(
            func_name=self.func_name(table),
            model_name=model_name,
            table=table,
            doc_params=doc_params,
            args=args,
            col_list=repr(col_list),
            num_cols=num_cols
        )

        return model_code, tool_code
