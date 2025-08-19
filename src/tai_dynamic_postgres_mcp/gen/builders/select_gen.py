from typing import List, Optional

from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen
from tai_dynamic_postgres_mcp.gen.schema.schema_parser import sql_columns_to_pydantic_model

_FUNC_PREFIX = "select"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import Optional, List
from pydantic import BaseModel
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.gen.templates.select import select_tmpl
from tai_dynamic_postgres_mcp.gen.filters.models import WhereFilter
from tai_dynamic_postgres_mcp.gen.order.models import OrderByItem

"""

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(where: Optional[WhereFilter] = None, order_by: Optional[List[OrderByItem]] = None, limit: Optional[int] = None) -> List[{model_name}]:
    """
    Selects rows from the `{table}` table.

    Parameters:
        where: Optional filters to apply using `WhereFilter`. For vector similarity (KNN), include in field filters like {{"vector_field": {{"knn": {{"query": [floats], "distance": "l2", "threshold": 0.5, "direction": "ASC"}}}}}}. If `threshold` is set, adds a distance filter; always implies ordering by distance (use 'direction' for ASC/DESC). Combine with AND/OR as needed.
        order_by: Optional list of fields and directions to order by (appended after any implied KNN orders).
        limit: Optional maximum number of rows to return.

    Returns:
        List of `{model_name}` objects from the `{table}` table.
    """
    
    return await select_tmpl("{table}", where, order_by, limit, {model_name})
'''


class SelectGen(BaseGen):
    def __init__(self, ignore_columns: Optional[List[str]] = None):
        super().__init__(_FUNC_PREFIX, _IMPORTS, _TOOL_TEMPLATE, ignore_columns)

    def generate_tool(
            self,
            table: str,
            columns: List[tuple],
    ) -> tuple[str, str]:
        select_columns = [(col, typ) for col, typ in columns if col not in self.ignore_columns]

        model_name, model_code = sql_columns_to_pydantic_model(self.prefix, table, select_columns)

        tool_code = self.template.format(
            func_name=self.func_name(table),
            model_name=model_name,
            table=table
        )

        return model_code, tool_code
