# Extreme Temperatures

How unusual is this period of weather compared to history?

A weather analysis tool built on NOAA GHCN Daily data. Computes rolling-window averages, climatology percentiles, and severity classifications for ~50 major US city stations.

## Architecture

- **Backend** (Python, FastAPI, DuckDB) — Ingests GHCN Daily data, computes rolling windows, climatology quantiles, records, and severity classifications. Serves a REST API.
- **Frontend** (Next.js, TypeScript, Tailwind) — Station list with severity indicators, station detail with insight card, temperature chart, seasonal rankings, and all-time records.

```
.
├── backend/             # FastAPI + DuckDB
│   ├── src/extreme_temps/
│   │   ├── api/         # FastAPI routers (stations, insights, series, records, rankings)
│   │   ├── compute/     # Rolling windows, climatology, severity, statements, rankings
│   │   ├── db/          # DuckDB schema, queries, connection
│   │   └── ingest/      # GHCN Daily, GSOD, Open-Meteo, station registry
│   └── tests/
├── frontend/            # Next.js + TypeScript
│   └── src/
│       ├── app/         # Pages (home, station detail)
│       ├── components/  # InsightCard, TemperatureChart, RecordsTable, etc.
│       └── lib/         # API client
├── scripts/             # Backfill and seed scripts
├── data/                # Station registry (stations.json)
├── notebooks/           # Marimo exploration notebook
└── src/weather_fetcher/ # Legacy v0.1 CLI
```

## Setup

```bash
# Backend
uv sync --project backend
uv run --project backend extreme-temps serve

# Frontend
cd frontend && npm install && npm run dev
```

## Data Pipeline

```bash
# Seed station registry
uv run --project backend python scripts/seed_stations.py

# Backfill a station (ingest + climatology + records + rolling windows + latest insights)
uv run --project backend python scripts/backfill.py --stations USW00094728

# Backfill all stations
uv run --project backend python scripts/backfill.py --stations all
```

## Testing

```bash
uv run --project backend pytest backend/tests/ -v
```

## Data Sources

- **GHCN Daily** (primary) — full historical record from NOAA
- **GSOD** (secondary) — gap-filler for missing GHCN data
- **Open-Meteo** (near-real-time) — fills the 3-5 day GHCN publication lag
