# tai_dynamic_postgres_mcp

This project is designed to dynamically generate PostgreSQL-aware tools for use in FastMCP-based agent systems.

## What It Does

- Connects to your PostgreSQL instance and introspects the schema.
- Dynamically generates FastMCP-compatible tools for PostgreSQL DML operations.
- Automatically creates Pydantic-based models to define the structure of inputs for each tool.
- Exposes those tools directly through FastMCP so they are usable by agents and workflows.
- Supports configurable column skipping for insert operations (e.g., auto-generated `id`, timestamps, etc.).

## Environment Variables

Set the following environment variables to configure your PostgreSQL connection and pooling:

```env
PG_HOST=localhost  
PG_PORT=5555  
PG_DB=nolie  
PG_USER=postgres  
PG_PASSWORD=

PG_POOL_MIN_SIZE=2  
PG_POOL_MAX_SIZE=20  
PG_POOL_TIMEOUT=5  
PG_POOL_MAX_LIFETIME=600
```

## Usage

Example: Run directly from Git using uvx

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/Frank-Tech/tai_dynamic_postgres_mcp.git",
        "tai-postgres-mcp", 
        "--skip-insert-column",
        "id",
        "--skip-insert-column",
        "date_created"
      ],
      "env": {
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_DB": "dbname",
        "PG_USER": "username",
        "PG_PASSWORD": "password",       
      }
    }
  }
}

```

## Limitations

- Currently only supports generation of `executemany`-style insert tools.
- No caching is implemented yet for the generated schema or models.
