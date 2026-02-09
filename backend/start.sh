#!/bin/sh
set -e

DB_PATH="${DATABASE_PATH:-/data/extreme_temps.duckdb}"
DB_DIR=$(dirname "$DB_PATH")

# Download pre-built DB on first boot if volume is empty
if [ ! -f "$DB_PATH" ]; then
    echo "Database not found at $DB_PATH — downloading from GitHub release..."
    mkdir -p "$DB_DIR"
    curl -fSL -o "$DB_PATH" \
        "https://github.com/ccerv1/extreme-temperatures/releases/download/v0.2.0/extreme_temps.duckdb"
    echo "Download complete ($(du -h "$DB_PATH" | cut -f1))"
fi

exec uvicorn extreme_temps.api.app:create_app --factory --host 0.0.0.0 --port ${PORT:-8000}
