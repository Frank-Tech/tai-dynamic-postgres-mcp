import re
from typing import List, Tuple

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

_MODEL_TEMPLATE = '''
class {model_name}(BaseModel):
{fields}

'''

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
    base_type = re.split(r'[\s()]', sql_type)[0]
    py_type = SQL_TO_PYTHON.get(base_type, 'Any')
    return f'Optional[{py_type}]' if nullable else py_type


def parse_schema(schema: str):
    tables = {}
    current_table = None

    for line in schema.splitlines():
        line = line.strip()

        table_match = re.match(r'CREATE TABLE (?:(\w+)\.)?(\w+)', line, re.IGNORECASE)
        if table_match:
            schema_name = table_match.group(1) or 'public'
            table_name = table_match.group(2)
            full_table_name = f"{schema_name}.{table_name}"
            current_table = full_table_name
            tables[current_table] = []
            continue

        if current_table and line.startswith(')'):
            current_table = None
            continue

        if current_table and '--' not in line and line:
            line = line.rstrip(',')  # remove trailing comma

            parts = line.split()
            col_name = parts[0]

            type_parts = []
            for part in parts[1:]:
                if part.upper() not in CONSTRAINT_KEYWORDS:
                    type_parts.append(part)

            col_type = ' '.join(type_parts)
            constraints = ' '.join(parts[1 + len(type_parts):])

            nullable = 'NOT NULL' not in constraints.upper()
            py_type = sql_type_to_python_type(col_type, nullable)
            tables[current_table].append((col_name, py_type))

    return tables


def sql_columns_to_pydantic_model(prefix: str, table: str, columns: List[Tuple[str, str]]) -> tuple[str, str]:
    name = f"{prefix}_{table}_row"
    model_name = ''.join(word.capitalize() for word in name.replace('.', '_').split('_'))
    fields = "\n".join([f"    {col}: {typ}" for col, typ in columns])
    return model_name, _MODEL_TEMPLATE.format(model_name=model_name, fields=fields)
