FROM mcr.microsoft.com/devcontainers/python:3.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    ca-certificates \
    curl \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# Pre-install Python deps in image layer (only re-runs when pyproject.toml changes)
COPY pyproject.toml /tmp/pyproject.toml
RUN pip install \
    "asyncpg>=0.29.0" "redis>=5.0" "confluent-kafka>=2.3" \
    "aiohttp>=3.9" "minio>=7.0" "prometheus-client>=0.20" \
    "clickhouse-connect>=0.7" \
    "pytest>=8.0" "pytest-asyncio>=0.24" "ruff>=0.8" "mypy>=1.13"

# The base Debian image ships a system Python 3.13 at /usr/bin/python3.13.
# VS Code's test adapter discovers it and uses it for pytest discovery,
# causing "No module named pytest" errors because deps are only installed
# in the devcontainer Python 3.12 at /usr/local/bin/python3.
# Remove the system Python 3.13 binary so VS Code can only find 3.12.
RUN rm -f /usr/bin/python3.13 /bin/python3.13 \
    && ln -sf /usr/local/bin/python3 /usr/bin/python3 \
    && ln -sf /usr/local/bin/python3 /bin/python3
