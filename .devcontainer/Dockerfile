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

# Grant the vscode user access to the Docker socket (mounted from host)
# so testcontainers can start Postgres, Redis, etc. for integration tests.
RUN groupadd -f docker && usermod -aG docker vscode

# The base Debian image ships a system Python 3.13 at /usr/bin/python3.13.
# VS Code's test adapter discovers it and uses it for pytest discovery,
# causing "No module named pytest" errors because deps are only installed
# in the devcontainer Python 3.12 at /usr/local/bin/python3.
# Remove the system Python 3.13 binary so VS Code can only find 3.12.
RUN rm -f /usr/bin/python3.13 /bin/python3.13 \
    && ln -sf /usr/local/bin/python3 /usr/bin/python3 \
    && ln -sf /usr/local/bin/python3 /bin/python3
