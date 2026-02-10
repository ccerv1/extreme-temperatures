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

## Adding a Station

To add a new weather station to the app:

### 1. Find the GHCN station ID

Search [NOAA Climate Data Online](https://www.ncdc.noaa.gov/cdo-web/search) for a station near the target city. Airport stations (prefix `USW`) work best — they have the longest, most reliable records. Note the station ID, lat/lon, elevation, and WBAN number (last 6 digits of the station ID).

### 2. Add to both station JSON files

Add an entry to **both** files (they must stay in sync):

- `data/stations.json`
- `frontend/src/data/stations.json`

```json
{"station_id": "USW00024225", "wban": "024225", "name": "Medford Rogue Valley International Airport", "city": "Medford", "location": "MFR Airport", "lat": 42.3750, "lon": -122.8770, "elevation_m": 400.3}
```

### 3. Backfill the station

This ingests historical data, computes climatology, records, rolling windows, and latest insights:

```bash
uv run --project backend python scripts/backfill.py --stations USW00024225
```

### 4. Verify locally

Start the backend and frontend, then check that the new station appears on the home page and its detail page loads correctly.

### 5. Deploy

```bash
# Upload updated database to GitHub release
gh release upload v0.2.0 data/extreme_temps.duckdb --clobber

# Deploy backend to Railway
railway up --detach

# Deploy frontend to Vercel (auto-deploys on push, or manually)
cd frontend && vercel --yes --prod

# Trigger production data refresh
curl -X POST https://extreme-temperatures-production.up.railway.app/insights/compute-latest
```

## Data Sources

- **GHCN Daily** (primary) — full historical record from NOAA
- **GSOD** (secondary) — gap-filler for missing GHCN data
- **Open-Meteo** (near-real-time) — fills the 3-5 day GHCN publication lag
