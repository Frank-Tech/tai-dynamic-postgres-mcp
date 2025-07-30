import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.gen.schema_parser import parse_schema

_OUTPUT_DIR = Path(tools.__file__).resolve().parent

_TOOLS_SUFFIX = "_tools"


class BaseGen(ABC):
    def __init__(
            self,
            prefix: str,
            imports: str,
            template: str,
            ignore_columns: Optional[List[str]] = None
    ) -> None:
        self.prefix = prefix
        self.imports = imports
        self.template = template
        self.ignore_columns = ignore_columns or []

    @abstractmethod
    def generate_tool(
            self,
            table: str,
            columns: List[tuple],
    ) -> tuple[str, str]:
        raise NotImplementedError

    @property
    def output_path(self) -> Path:
        return _OUTPUT_DIR / f'{self.prefix}_{_TOOLS_SUFFIX}.py'

    @property
    def is_exists(self) -> bool:
        return self.output_path.exists()

    def func_name(self, table: str) -> str:
        return f"{self.prefix}_{table.replace('.', '_')}"

    def generate_tools(self, schema: str) -> List[str]:
        tables = parse_schema(schema)
        chunks = [self.imports]
        for table, columns in tables.items():
            model_code, tool_code = self.generate_tool(table, columns)
            chunks.append(model_code)
            chunks.append(tool_code)

        return chunks

    def generate_file(self, schema: str):
        with open(self.output_path, "w") as f:
            for chunk in self.generate_tools(schema):
                f.write(chunk)
        os.chmod(self.output_path, 0o444)
