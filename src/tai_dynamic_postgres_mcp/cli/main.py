import asyncio
import sys

from tai_dynamic_postgres_mcp.cli.args_parser import build_parser
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.database.connection import close_connection_pool, get_async_connection
from tai_dynamic_postgres_mcp.gen.loader import load_dynamic_tools

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def runner():
    parser = build_parser()
    args = parser.parse_args()

    await load_dynamic_tools(
        overwrite=args.overwrite,
        ignore_insert_columns=args.ignore_insert_column,
        ignore_select_columns=args.ignore_select_column,
        ignore_update_columns=args.ignore_update_column,
        ignore_select_joined_columns=args.ignore_select_joined_column,
        select_joined=[group.split(',') for group in args.select_joined or []]
    )

    # ping pg
    with get_async_connection():
        pass

    if args.transport == "stdio":
        await mcp_app.run_async(transport=args.transport)
    else:
        await mcp_app.run_async(transport=args.transport, host=args.host, port=args.port)

    await close_connection_pool()


def main():
    return asyncio.run(runner())


if __name__ == "__main__":
    main()
