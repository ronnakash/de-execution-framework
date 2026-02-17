FROM mcr.microsoft.com/devcontainers/python:3.12

RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*
