import importlib
import logging
import pkgutil
from typing import Optional, List

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.gen.executemany_gen import generate_file as generate_executemany_file

_TOOLS_SUFFIX = "_tools"

logger = logging.getLogger(__name__)


def generate_tools(
        schema: str,
        ignore_insert_columns: Optional[List[str]] = None
):
    generate_executemany_file(schema, ignore_insert_columns)


def load_dynamic_tools(
        schema: str,
        ignore_insert_columns: Optional[List[str]] = None
):
    generate_tools(schema, ignore_insert_columns)
    for loader, module_name, is_pkg in pkgutil.walk_packages(tools.__path__, tools.__name__ + "."):
        try:
            if module_name.endswith(_TOOLS_SUFFIX):
                importlib.import_module(module_name)
        except ImportError as e:
            logger.warning(f"Failed to import {module_name}: {e}")
