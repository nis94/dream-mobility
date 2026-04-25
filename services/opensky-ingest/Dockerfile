# syntax=docker/dockerfile:1.7
#
# Two-stage Python build using uv. Mirrors services/archiver/Dockerfile.

FROM ghcr.io/astral-sh/uv:0.5-python3.13-bookworm-slim AS build
WORKDIR /app/services/opensky-ingest
COPY services/opensky-ingest/pyproject.toml services/opensky-ingest/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 nonroot \
    && useradd -u 65532 -g 65532 -M -s /usr/sbin/nologin nonroot
WORKDIR /app/services/opensky-ingest
COPY --from=build /app/services/opensky-ingest/.venv /app/services/opensky-ingest/.venv
COPY services/opensky-ingest/opensky_ingest.py ./opensky_ingest.py
ENV PATH="/app/services/opensky-ingest/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER 65532:65532
ENTRYPOINT ["/app/services/opensky-ingest/.venv/bin/python", "opensky_ingest.py"]
