# tai-dynamic-postgres-mcp

##### Schema-Based Generator for PostgreSQL DML Tools in FastMCP Agent Systems

## What It Does

- Connects to your PostgreSQL instance and introspects the schema.
- Generates one FastMCP-compatible tool per DML operation (insert, select, update, delete) for each table.
- Creates a dedicated Pydantic model for each tool's input, based on the table’s structure.
- Supports column exclusion for the relevant tools (e.g., `id`, `created_at`, etc.).
- Limits agent access to only generated tools, preventing unrestricted SQL or schema changes.

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

Basic Example: Run directly from Git using uvx

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uvx",
      "args": [
        "--from",
        "git+https://github.com/Frank-Tech/tai-dynamic-postgres-mcp.git",
        "tai-postgres-mcp"
      ],
      "env": {
        "PG_HOST": "localhost",
        "PG_PORT": "5432",
        "PG_DB": "dbname",
        "PG_USER": "username",
        "PG_PASSWORD": "password"
      }
    }
  }
}
```
