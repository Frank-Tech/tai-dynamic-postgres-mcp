import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List

from tai_dynamic_postgres_mcp import tools
from tai_dynamic_postgres_mcp.gen.schema.schema_parser import parse_schema

_OUTPUT_DIR = Path(tools.__file__).resolve().parent

TOOLS_SUFFIX = "tools"


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
        return _OUTPUT_DIR / f'{self.prefix}_{TOOLS_SUFFIX}.py'

    @property
    def is_exists(self) -> bool:
        return self.output_path.exists()

    def func_name(self, table: str) -> str:
        return f"{self.prefix}_{table.replace('.', '_')}"

    def generate_tools(self, schema: str) -> List[str]:
        tables, fks = parse_schema(schema)
        chunks = [self.imports]
        for table, columns in tables.items():
            model_code, tool_code = self.generate_tool(table, columns)
            chunks.append(model_code)
            chunks.append(tool_code)

        return chunks

    def generate_file(self, schema: str):
        if self.is_exists:
            os.chmod(self.output_path, 0o644)

        with self.output_path.open("w") as f:
            for chunk in self.generate_tools(schema):
                # Some generators (like DeleteGen) don't generate model code,
                # so `chunk` can be an empty string. This avoids writing blank lines.
                if chunk:
                    f.write(chunk)
        os.chmod(self.output_path, 0o444)
