from argparse import ArgumentParser


def build_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description="Generate dynamic insert MCP tools based on a given PostgreSQL schema."
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the generated tool file if it already exists."
    )

    parser.add_argument(
        '--select-joined',
        action='append',
        help="Join groups for select, e.g., --select_joined=table1,table2,table3"
    )

    parser.add_argument(
        "--ignore-insert-column",
        action="append",
        default=["id", "date_created", "date_updated"],
        help="Columns to ignore when generating insert functions. Can be specified multiple times."
    )

    parser.add_argument(
        "--ignore-select-column",
        action="append",
        default=[],
        help="Columns to ignore when generating select functions. Can be specified multiple times."
    )

    parser.add_argument(
        "--ignore-update-column",
        action="append",
        default=["id", "date_created"],
        help="Columns to ignore when generating update functions. Can be specified multiple times."
    )

    parser.add_argument(
        "--ignore-select-joined-column",
        action="append",
        default=[],
        help="Columns to ignore when generating select_joined functions. Can be specified multiple times."
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
