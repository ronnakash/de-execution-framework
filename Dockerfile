FROM python:3.12-slim AS base

# System dependencies for building native extensions (asyncpg, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project metadata and source first for better layer caching.
# When only application code changes, pip install layer is cached.
COPY pyproject.toml README.md ./
COPY de_platform/ de_platform/

# Install with all infra dependencies (asyncpg, redis, confluent-kafka, minio, clickhouse-connect)
RUN pip install --no-cache-dir '.[infra]'

# Default entrypoint: the platform CLI
ENTRYPOINT ["python", "-m", "de_platform"]
# Default command: show help
CMD ["--help"]
