from typing import List

from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen

_FUNC_PREFIX = "delete"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import Optional
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.gen.templates.delete import delete_tmpl
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter

"""

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(where: Optional[WhereFilter] = None) -> int:
    """
    Deletes rows from the `{table}` table.

    Parameters:
        where: Optional filters to apply using `WhereFilter`.

    Returns:
        Number of rows deleted from the `{table}` table.
    """

    return await delete_tmpl("{table}", where)
'''


class DeleteGen(BaseGen):
    def __init__(self):
        super().__init__(_FUNC_PREFIX, _IMPORTS, _TOOL_TEMPLATE)

    def generate_tool(
            self,
            table: str,
            _: List[tuple],  # columns are unused here but kept for interface compatibility
    ) -> tuple[str, str]:
        tool_code = self.template.format(
            func_name=self.func_name(table),
            table=table
        )
        return "", tool_code  # No model needed for delete
