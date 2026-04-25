# syntax=docker/dockerfile:1.7
#
# Two-stage Python build using uv. The build stage resolves the locked deps
# into an isolated venv, the runtime stage copies only the venv + source so
# the image stays close to the python:3.13-slim base.
#
# Layout note: archiver.py loads the Avro schema via the relative path
# `../../internal/avro/movement_event.avsc` (computed from __file__). To keep
# the source untouched we reproduce that tree inside the image:
#   /app/services/archiver/archiver.py
#   /app/internal/avro/movement_event.avsc
# and set WORKDIR to /app/services/archiver so the relative lookup resolves.

# ── Stage 1: resolve deps into a venv ───────────────────────────────────────
FROM ghcr.io/astral-sh/uv:0.5-python3.13-bookworm-slim AS build

WORKDIR /app/services/archiver

# uv creates the venv at ./.venv by default; copy only what's needed for
# dependency resolution first so this layer caches across source edits.
COPY services/archiver/pyproject.toml services/archiver/uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# ── Stage 2: minimal runtime ────────────────────────────────────────────────
FROM python:3.13-slim

# procps gives us pgrep for the liveness probe; curl is kept off to keep the
# image small. Run as non-root uid 65532 to match the other dream-flight
# charts' securityContext.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 65532 nonroot \
    && useradd -u 65532 -g 65532 -M -s /usr/sbin/nologin nonroot

WORKDIR /app/services/archiver

COPY --from=build /app/services/archiver/.venv /app/services/archiver/.venv
COPY services/archiver/archiver.py ./archiver.py
# Mirrors the host-side repo layout so archiver.py's relative schema path
# (`../../internal/avro/movement_event.avsc`) resolves unchanged.
COPY internal/avro/flight_telemetry.avsc /app/internal/avro/flight_telemetry.avsc

ENV PATH="/app/services/archiver/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 65532:65532

ENTRYPOINT ["/app/services/archiver/.venv/bin/python", "archiver.py"]
