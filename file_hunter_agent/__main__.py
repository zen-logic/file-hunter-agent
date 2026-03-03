"""CLI entry point for the File Hunter agent."""

import argparse
import logging
import sys

import uvicorn

from file_hunter_agent import config


def main():
    parser = argparse.ArgumentParser(
        description="File Hunter Agent — remote filesystem agent"
    )
    parser.add_argument(
        "--server",
        help="Central server URL (e.g. http://fileserver:8000). Required on first run.",
    )
    parser.add_argument(
        "--token",
        help="Pairing token from the server. Required on first run.",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="HTTP server bind address (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="HTTP server port (default: 8001)",
    )
    args = parser.parse_args()

    # Load saved config
    config.load_config()

    # Apply CLI overrides and save
    updates = {}
    if args.server:
        updates["server_url"] = args.server.rstrip("/")
    if args.token:
        updates["token"] = args.token
    if args.host is not None:
        updates["http_host"] = args.host
    if args.port is not None:
        updates["http_port"] = args.port

    if updates:
        config.save_config(updates)

    # Validate required config
    if not config.get("server_url"):
        print("Error: --server is required on first run.", file=sys.stderr)
        sys.exit(1)
    if not config.get("token"):
        print("Error: --token is required on first run.", file=sys.stderr)
        sys.exit(1)

    host = config.get("http_host", "0.0.0.0")
    port = config.get("http_port", 8001)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    logger = logging.getLogger("file_hunter_agent")
    logger.info("File Hunter Agent starting")
    logger.info("Server: %s", config.get("server_url"))
    logger.info("HTTP: %s:%d", host, port)

    locations = config.get_locations()
    if locations:
        logger.info("Locations (%d):", len(locations))
        for loc in locations:
            logger.info("  %s -> %s", loc["name"], loc["path"])
    else:
        logger.warning("No locations configured. Edit config.json to add locations.")

    from file_hunter_agent.app import create_app

    app = create_app()
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
