from pathlib import Path
from typing import List, Optional

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.gen.schema_parser import parse_schema, sql_columns_to_pydantic_model

_FUNC_PREFIX = "executemany"

_OUTPUT_FILEPATH = Path(tools.__file__).resolve().parent / "executemany_tools.py"

_IMPORTS = """# This file is auto-generated. Do not edit manually.

from typing import *
from pydantic import BaseModel
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.tools.helpers import executemany_tmpl

"""

_MODEL_TEMPLATE = '''
class {model_name}(BaseModel):
{fields}
'''

_TOOL_TEMPLATE = '''
@mcp_app.tool
async def {func_name}(params: List[{model_name}], raise_on_conflicts: bool = True) -> List[str]:
    """
    Inserts multiple rows into the `{table}` table.

    Parameters:
        params: List of `{model_name}` objects in the order: {doc_params}

    Returns:
        List of inserted row IDs from the `{table}` table.
    """

    values = [({args}) for row in params or []]

    return await executemany_tmpl("{table}", {col_list}, values, raise_on_conflicts)
'''


def generate_function_code(
        table: str,
        columns: List[tuple],
        ignore_insert_columns: Optional[List[str]] = None
) -> tuple[str, str]:
    func_name = f"{_FUNC_PREFIX}_{table.replace('.', '_')}"

    ignore_insert_columns = ignore_insert_columns or []
    insert_columns = [(col, typ) for col, typ in columns if col not in ignore_insert_columns]

    model_name, model_code = sql_columns_to_pydantic_model(_FUNC_PREFIX, table, insert_columns)

    doc_params = ', '.join([col for col, _ in insert_columns])
    args = ', '.join([f"row.{col}" for col, _ in insert_columns])
    col_list = [col for col, _ in insert_columns]
    num_cols = len(insert_columns)

    tool_code = _TOOL_TEMPLATE.format(
        func_name=func_name,
        model_name=model_name,
        table=table,
        doc_params=doc_params,
        args=args,
        col_list=repr(col_list),
        num_cols=num_cols
    )

    return model_code, tool_code


def generate_file(schema: str, ignore_insert_columns: Optional[List[str]] = None):
    tables = parse_schema(schema)

    with open(_OUTPUT_FILEPATH, 'w') as f:
        f.write(_IMPORTS)

        for table, columns in tables.items():
            model_code, tool_code = generate_function_code(table, columns, ignore_insert_columns)
            f.write(model_code)
            f.write(tool_code)
            f.write("\n")
