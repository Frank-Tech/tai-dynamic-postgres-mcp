import asyncio
import sys

from tai_dynamic_postgres_mcp.cli.args_parser import build_parser
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
    async with get_async_connection():
        pass

    try:
        if transport == "stdio":
            await mcp.run_async(transport=transport)
        else:
            await mcp.run_async(transport=transport, host=mcp_host, port=mcp_port)
    except KeyboardInterrupt as e:
        logging.info("KeyboardInterrupt")
        return 120
    except Exception as e:
        logging.error(str(e))
        return 1
    finally:
        await close_connection_pool()


def main():
    sys.exit(asyncio.run(runner()))


if __name__ == "__main__":
    main()
