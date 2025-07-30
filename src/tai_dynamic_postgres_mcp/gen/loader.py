import importlib
import logging
import pkgutil
from typing import Optional, List

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.database.ddl import generate_schema_ddl
from tai_dynamic_postgres_mcp.gen.base_gen import BaseGen, _TOOLS_SUFFIX
from tai_dynamic_postgres_mcp.gen.executemany_gen import ExecuteManyGen

logger = logging.getLogger(__name__)


async def generate_tools(
        overwrite: bool = True,
        ignore_insert_columns: Optional[List[str]] = None
):
    gen_list: List[BaseGen] = [
        ExecuteManyGen(ignore_insert_columns),
    ]

    to_generate = gen_list if overwrite else [gen for gen in gen_list if not gen.is_exists]
    if not to_generate:
        return

    schema = await generate_schema_ddl()
    for gen in to_generate:
        gen.generate_file(schema)


async def load_dynamic_tools(
        overwrite: bool = True,
        ignore_insert_columns: Optional[List[str]] = None
):
    await generate_tools(overwrite, ignore_insert_columns)
    for loader, module_name, is_pkg in pkgutil.walk_packages(tools.__path__, tools.__name__ + "."):
        try:
            if module_name.endswith(_TOOLS_SUFFIX):
                importlib.import_module(module_name)
        except ImportError as e:
            logger.warning(f"Failed to import {module_name}: {e}")
