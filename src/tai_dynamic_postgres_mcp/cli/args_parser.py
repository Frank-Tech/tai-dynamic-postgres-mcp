from argparse import ArgumentParser


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Generate dynamic insert MCP tools based on a given PostgreSQL schema."
    )

    parser.add_argument(
        "--ignore-insert-column",
        action="append",
        default=["id", "date_created", "date_updated"],
        help="Columns to skip when generating insert functions. Can be specified multiple times."
    )

    parser.add_argument(
        "-t", "--transport",
        choices=["stdio", "http", "sse", "streamable-http"],
        default="stdio",
        help="Transport mechanism for the MCP server: "
             "'stdio' for CLI interaction, 'sse' for Server-Sent Events, "
             "'http' for REST API, 'streamable-http' for streamable REST API responses. "
             "Defaults to 'stdio'."
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to (used only with HTTP/SSE transports). Default is 127.0.0.1."
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port number to bind the server to (used only with HTTP/SSE transports). Default is 8000."
    )

    return parser
