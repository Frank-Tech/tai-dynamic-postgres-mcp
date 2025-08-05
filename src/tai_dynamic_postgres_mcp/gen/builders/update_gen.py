from typing import List, Optional

from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen
from tai_dynamic_postgres_mcp.gen.schema.schema_parser import sql_columns_to_pydantic_model

_FUNC_PREFIX = "update"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import Optional, List
from pydantic import BaseModel
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.gen.templates.update import update_tmpl
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter

"""

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(data: {model_name}, where: Optional[WhereFilter] = None) -> int:
    """
    Updates rows in the `{table}` table.

    Parameters:
        data: Partial `{model_name}` object with fields to update.
        where: Optional filters to apply using `WhereFilter`.

    Returns:
        Number of rows updated in the `{table}` table.
    """
    return await update_tmpl("{table}", data, where)
'''


class UpdateGen(BaseGen):
    def __init__(self, ignore_columns: Optional[List[str]] = None):
        super().__init__(_FUNC_PREFIX, _IMPORTS, _TOOL_TEMPLATE, ignore_columns)

    def generate_tool(
            self,
            table: str,
            columns: List[tuple],
    ) -> tuple[str, str]:
        update_columns = [(col, typ) for col, typ in columns if col not in self.ignore_columns]

        # All fields optional for updates
        optional_columns = [(col, f"Optional[{typ}]") for col, typ in update_columns]

        model_name, model_code = sql_columns_to_pydantic_model(self.prefix, table, optional_columns)

        tool_code = self.template.format(
            func_name=self.func_name(table),
            model_name=model_name,
            table=table
        )

        return model_code, tool_code
