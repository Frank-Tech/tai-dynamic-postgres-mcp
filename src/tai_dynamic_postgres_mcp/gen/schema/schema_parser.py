import re
from typing import Dict, List, Tuple, Optional

CONSTRAINT_KEYWORDS = {
    'NOT', 'NULL',  # NOT NULL, NULL
    'DEFAULT',  # DEFAULT value
    'PRIMARY',  # PRIMARY KEY
    'UNIQUE',  # UNIQUE constraint
    'CHECK',  # CHECK (...)
    'REFERENCES',  # REFERENCES table(col)
    'GENERATED',  # GENERATED ALWAYS AS IDENTITY
    'COLLATE',  # COLLATE something
    'CONSTRAINT'  # CONSTRAINT name ...
}

SQL_TO_PYTHON = {
    # Integer types
    'serial': 'int',
    'bigserial': 'int',
    'smallserial': 'int',
    'smallint': 'int',
    'integer': 'int',
    'int': 'int',
    'bigint': 'int',

    # Decimal types
    'real': 'float',
    'double precision': 'float',
    'numeric': 'float',
    'decimal': 'float',

    # Boolean
    'boolean': 'bool',
    'bool': 'bool',

    # Character types
    'text': 'str',
    'varchar': 'str',
    'character varying': 'str',
    'char': 'str',
    'character': 'str',
    'uuid': 'str',

    # Temporal types as strings
    'date': 'str',
    'timestamp': 'str',
    'timestamp without time zone': 'str',
    'timestamp with time zone': 'str',
    'timestamptz': 'str',
    'time': 'str',
    'time without time zone': 'str',
    'time with time zone': 'str',

    # JSON types
    'json': 'dict',
    'jsonb': 'dict',

    # Arrays and special
    'bytea': 'bytes',
    'inet': 'str',
    'cidr': 'str',
    'macaddr': 'str',
    'interval': 'str',

    # PostgreSQL array types
    'integer[]': 'List[int]',
    'int[]': 'List[int]',
    'bigint[]': 'List[int]',
    'smallint[]': 'List[int]',
    'text[]': 'List[str]',
    'varchar[]': 'List[str]',
    'character varying[]': 'List[str]',
    'boolean[]': 'List[bool]',
    'bool[]': 'List[bool]',
    'uuid[]': 'List[str]',
    'numeric[]': 'List[float]',
    'real[]': 'List[float]',
    'double precision[]': 'List[float]',
    'json[]': 'List[dict]',
    'jsonb[]': 'List[dict]',

    # Catch-all fallback
    'any': 'Any',
}


def sql_type_to_python_type(sql_type: str, nullable: bool) -> str:
    sql_type = sql_type.lower().strip()
    # Handle array types
    if sql_type.endswith('[]'):
        base_type = sql_type[:-2]
        py_base = SQL_TO_PYTHON.get(base_type, 'Any')
        py_type = f'List[{py_base}]'
    else:
        base_type = re.split(r'[\s()]', sql_type)[0]
        py_type = SQL_TO_PYTHON.get(base_type, 'Any')
    return f'Optional[{py_type}]' if nullable else py_type


def parse_schema(schema: str) -> Tuple[Dict[str, List[Tuple[str, str]]], List[Tuple[str, str, str, str]]]:
    tables: Dict[str, List[Tuple[str, str]]] = {}
    fks: List[Tuple[str, str, str, str]] = []  # (table, col, ref_table, ref_col)
    current_table: Optional[str] = None

    lines = schema.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()

        table_match = re.match(r'CREATE TABLE (?:(\w+)\.)?(\w+)', line, re.IGNORECASE)
        if table_match:
            schema_name = table_match.group(1) or 'public'
            table_name = table_match.group(2)
            full_table_name = f"{schema_name}.{table_name}"
            current_table = full_table_name
            tables[current_table] = []
            # Skip to the opening parenthesis if on next line
            i += 1
            continue

        if current_table and line.startswith(')'):
            current_table = None
            i += 1
            continue

        if current_table and line and not line.startswith('--'):
            line = line.rstrip(',')  # remove trailing comma
            if not line or line == '(':
                i += 1
                continue

            if line.upper().startswith('FOREIGN KEY'):
                fk_match = re.match(r'FOREIGN\s+KEY\s*\((.*?)\)\s*REFERENCES\s*(?:(\w+)\.)?(\w+)\s*\((.*?)\)\s*(?:.*)',
                                    line, re.IGNORECASE)
                if fk_match:
                    fk_col = fk_match.group(1).strip()
                    ref_schema = fk_match.group(2) or 'public'
                    ref_table = fk_match.group(3)
                    ref_col = fk_match.group(4).strip()
                    full_ref_table = f"{ref_schema}.{ref_table}"
                    fks.append((current_table, fk_col, full_ref_table, ref_col))
                i += 1
                continue

            if line.upper().startswith('UNIQUE') or line.upper().startswith('PRIMARY KEY') or line.upper().startswith(
                    'CHECK'):
                # Skip table constraints that are not FK
                i += 1
                continue

            # Column definition
            parts = re.split(r'\s+', line)
            col_name = parts[0]

            type_parts = []
            j = 1
            while j < len(parts) and parts[j].upper() not in CONSTRAINT_KEYWORDS:
                type_parts.append(parts[j])
                j += 1

            col_type = ' '.join(type_parts)
            constraints = ' '.join(parts[j:])

            nullable = 'NOT NULL' not in constraints.upper()
            if 'PRIMARY KEY' in constraints.upper():
                nullable = False
            py_type = sql_type_to_python_type(col_type, nullable)
            tables[current_table].append((col_name, py_type))

            # Handle inline FOREIGN KEY (REFERENCES in constraints)
            fk_inline_match = re.search(r'REFERENCES\s*(?:(\w+)\.)?(\w+)\s*\((\w+)\)\s*(?:.*)', constraints,
                                        re.IGNORECASE)
            if fk_inline_match:
                ref_schema = fk_inline_match.group(1) or 'public'
                ref_table = fk_inline_match.group(2)
                ref_col = fk_inline_match.group(3)
                full_ref_table = f"{ref_schema}.{ref_table}"
                fks.append((current_table, col_name, full_ref_table, ref_col))

        i += 1

    return tables, fks


def sql_columns_to_pydantic_model(prefix: str, table: str, columns: List[Tuple[str, str]]) -> tuple[str, str]:
    name = f"{prefix}_{table}_row"
    model_name = ''.join(word.capitalize() for word in name.replace('.', '_').split('_'))
    fields = "\n".join([f"    {col}: {typ}" for col, typ in columns])
    return model_name, '''
class {model_name}(BaseModel):
{fields}

'''.format(model_name=model_name, fields=fields)
