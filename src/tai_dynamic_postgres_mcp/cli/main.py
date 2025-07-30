import asyncio

from tai_dynamic_postgres_mcp.cli.args_parser import build_parser
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.database.connection import close_connection_pool
from tai_dynamic_postgres_mcp.database.ddl import generate_schema_ddl
from tai_dynamic_postgres_mcp.gen.gen import load_dynamic_tools


async def runner():
    parser = build_parser()
    args = parser.parse_args()

    schema = await generate_schema_ddl()
    load_dynamic_tools(schema, args.ignore_insert_column)

    if args.transport == "stdio":
        await mcp_app.run_async(transport=args.transport)
    else:
        await mcp_app.run_async(transport=args.transport, host=args.host, port=args.port)

    await close_connection_pool()


def main():
    return asyncio.run(runner())


if __name__ == "__main__":
    main()
