"""CLI entry point for extreme-temps."""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="extreme-temps",
        description="Historical weather extremes — ingest, compute, and serve",
    )
    subparsers = parser.add_subparsers(dest="command")

    # ingest subcommand
    ingest_parser = subparsers.add_parser("ingest", help="Ingest weather data")
    ingest_parser.add_argument("--station", required=True, help="GHCN station ID (e.g. USW00094728)")
    ingest_parser.add_argument("--full", action="store_true", help="Full historical backfill")
    ingest_parser.add_argument("--incremental", action="store_true", help="Incremental update since last ingest")

    # compute subcommand
    compute_parser = subparsers.add_parser("compute", help="Compute analytics")
    compute_parser.add_argument("--station", required=True, help="GHCN station ID")
    compute_parser.add_argument("--all", action="store_true", help="Compute all analytics (windows, climatology, records)")

    # serve subcommand
    subparsers.add_parser("serve", help="Start the FastAPI server")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "serve":
        _serve()
    elif args.command == "ingest":
        _ingest(args)
    elif args.command == "compute":
        _compute(args)


def _serve() -> None:
    import os
    try:
        import uvicorn
        from extreme_temps.api.app import create_app  # noqa: F401

        port = int(os.environ.get("PORT", "8000"))
        uvicorn.run("extreme_temps.api.app:create_app", factory=True, host="0.0.0.0", port=port)
    except ImportError as e:
        print(f"Missing dependency: {e}", file=sys.stderr)
        sys.exit(1)


def _ingest(args: argparse.Namespace) -> None:
    # Placeholder — implemented in Phase 2
    print(f"Ingest: station={args.station} full={args.full} incremental={args.incremental}")
    print("Not yet implemented. See Phase 2.")


def _compute(args: argparse.Namespace) -> None:
    # Placeholder — implemented in Phase 3
    print(f"Compute: station={args.station} all={args.all}")
    print("Not yet implemented. See Phase 3.")


if __name__ == "__main__":
    main()
