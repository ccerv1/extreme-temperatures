FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (cached layer)
COPY backend/pyproject.toml pyproject.toml
COPY backend/README.md README.md
COPY backend/src/ src/
RUN uv sync --no-dev

# Copy station registry and startup script
COPY data/stations.json /app/data/stations.json
COPY backend/start.sh start.sh

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["sh", "start.sh"]
