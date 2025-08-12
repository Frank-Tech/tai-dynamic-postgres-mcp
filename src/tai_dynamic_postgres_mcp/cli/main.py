import asyncio
import logging
import sys
from argparse import ArgumentTypeError

from tai_dynamic_postgres_mcp.cli.args_parser import build_parser
from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.database.connection import close_connection_pool, get_async_connection
from tai_dynamic_postgres_mcp.gen.loader import load_dynamic_tools

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def runner():
    parser = build_parser()
    args = parser.parse_args()
    if args.transport == "stdio":
        if args.host != "127.0.0.1" or args.port != 8000:
            raise ArgumentTypeError("Host and port should not be set when using 'stdio' transport.")

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
        if args.transport == "stdio":
            await mcp_app.run_async(transport=args.transport)
        else:
            await mcp_app.run_async(transport=args.transport, host=args.host, port=args.port)
    except KeyboardInterrupt as e:
        logging.info("KeyboardInterrupt")
        return 130
    except Exception as e:
        logging.error(str(e))
        return 1
    finally:
        try:
            await close_connection_pool()
        except asyncio.exceptions.CancelledError:
            logging.debug("CancelledError during connection pool closure, ignoring...")
        except Exception as e:
            logging.error(f"Error during connection pool closure: {str(e)}")


def main():
    sys.exit(asyncio.run(runner()))


if __name__ == "__main__":
    main()
