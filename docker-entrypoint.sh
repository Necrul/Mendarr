#!/bin/sh
set -eu

HOST="${MENDARR_HOST:-0.0.0.0}"
PORT="${MENDARR_PORT:-8095}"
APP_USER="${MENDARR_APP_USER:-mendarr}"
APP_GROUP="${MENDARR_APP_GROUP:-mendarr}"
DATA_DIR="${MENDARR_DATA_DIR:-/data}"

mkdir -p "$DATA_DIR"
chown -R "$APP_USER:$APP_GROUP" "$DATA_DIR" /app

exec gosu "$APP_USER:$APP_GROUP" python -m uvicorn app.main:app --host "$HOST" --port "$PORT"
