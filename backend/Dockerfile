FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Install dependencies first (cached layer)
COPY backend/pyproject.toml pyproject.toml
COPY backend/README.md README.md
COPY backend/src/ src/
RUN uv sync --no-dev

# Copy station registry
COPY data/stations.json /app/data/stations.json

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD uvicorn extreme_temps.api.app:create_app --factory --host 0.0.0.0 --port ${PORT:-8000}
