import importlib
import logging
import pkgutil
from typing import Optional, List

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.gen.builders.base_gen import BaseGen, TOOLS_SUFFIX
from tai_dynamic_postgres_mcp.gen.builders.delete_gen import DeleteGen
from tai_dynamic_postgres_mcp.gen.builders.insert_gen import InsertGen
from tai_dynamic_postgres_mcp.gen.builders.select_gen import SelectGen
from tai_dynamic_postgres_mcp.gen.builders.update_gen import UpdateGen
from tai_dynamic_postgres_mcp.gen.schema.introspect import generate_schema_ddl

logger = logging.getLogger(__name__)


async def load_dynamic_tools(
        overwrite: bool = True,
        ignore_insert_columns: Optional[List[str]] = None,
        ignore_select_columns: Optional[List[str]] = None,
        ignore_update_columns: Optional[List[str]] = None,
):
    gen_list: List[BaseGen] = [
        InsertGen(ignore_insert_columns),
        SelectGen(ignore_select_columns),
        UpdateGen(ignore_update_columns),
        DeleteGen(),
    ]

    to_generate = gen_list if overwrite else [gen for gen in gen_list if not gen.is_exists]
    if to_generate:
        schema = await generate_schema_ddl()
        for gen in to_generate:
            gen.generate_file(schema)

    for loader, module_name, is_pkg in pkgutil.walk_packages(tools.__path__, tools.__name__ + "."):
        try:
            if module_name.endswith(TOOLS_SUFFIX):
                importlib.import_module(module_name)
        except ImportError as e:
            logger.warning(f"Failed to import {module_name}: {e}")
