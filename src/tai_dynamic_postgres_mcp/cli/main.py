import asyncio
import logging
import sys

import click

from tai_dynamic_postgres_mcp.core.app import mcp_app
from tai_dynamic_postgres_mcp.database.connection import close_connection_pool, get_async_connection
from tai_dynamic_postgres_mcp.gen.loader import load_dynamic_tools

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def runner(
        overwrite: bool,
        readonly: bool,
        select_joined: tuple[str],
        ignore_insert_column: tuple[str],
        ignore_select_column: tuple[str],
        ignore_update_column: tuple[str],
        ignore_select_joined_column: tuple[str],
        transport: str,
        host: str,
        port: int,
):
    # Validate transport-related arguments
    if transport == "stdio":
        if host != "127.0.0.1" or port != 8000:
            raise click.BadParameter(
                "Host and port should not be set when using 'stdio' transport."
            )

    await load_dynamic_tools(
        overwrite=overwrite,
        readonly=readonly,
        ignore_insert_columns=ignore_insert_column,
        ignore_select_columns=ignore_select_column,
        ignore_update_columns=ignore_update_column,
        ignore_select_joined_columns=ignore_select_joined_column,
        select_joined=[group.split(",") for group in select_joined or []],
    )

    # ping pg
    async with get_async_connection():
        pass

    try:
        if transport == "stdio":
            await mcp_app.run_async(transport=transport)
        else:
            await mcp_app.run_async(transport=transport, host=host, port=port)
    except KeyboardInterrupt:
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


@click.command()
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite the generated tool file if it already exists.",
)
@click.option(
    "--readonly",
    is_flag=True,
    help="Generate only read-only tools (e.g., select functions). No insert or update tools will be generated.",
)
@click.option(
    "--select-joined",
    multiple=True,
    help="Join groups for select, e.g., --select-joined table1,table2,table3",
)
@click.option(
    "--ignore-insert-column",
    multiple=True,
    default=["id", "date_created", "date_updated"],
    show_default=True,
    help="Columns to ignore when generating insert functions.",
)
@click.option(
    "--ignore-select-column",
    multiple=True,
    default=[],
    help="Columns to ignore when generating select functions.",
)
@click.option(
    "--ignore-update-column",
    multiple=True,
    default=["id", "date_created"],
    show_default=True,
    help="Columns to ignore when generating update functions.",
)
@click.option(
    "--ignore-select-joined-column",
    multiple=True,
    default=[],
    help="Columns to ignore when generating select_joined functions.",
)
@click.option(
    "-t",
    "--transport",
    type=click.Choice(["stdio", "http", "sse", "streamable-http"]),
    default="stdio",
    show_default=True,
    help="Transport mechanism for the MCP server.",
)
@click.option(
    "--host",
    default="127.0.0.1",
    show_default=True,
    help="Host to bind the server to (used only with HTTP/SSE transports).",
)
@click.option(
    "--port",
    default=8000,
    show_default=True,
    type=int,
    help="Port number to bind the server to (used only with HTTP/SSE transports).",
)
def main(
        overwrite,
        readonly,
        select_joined,
        ignore_insert_column,
        ignore_select_column,
        ignore_update_column,
        ignore_select_joined_column,
        transport,
        host,
        port,
):
    """Generate dynamic insert MCP tools based on a given PostgreSQL schema."""
    sys.exit(asyncio.run(runner(
        overwrite,
        readonly,
        select_joined,
        ignore_insert_column,
        ignore_select_column,
        ignore_update_column,
        ignore_select_joined_column,
        transport,
        host,
        port,
    )))


if __name__ == "__main__":
    main()
