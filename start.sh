#!/bin/bash
set -e

echo "[$(date)] Starting dagster-daemon..."
dagster-daemon run &
DAEMON_PID=$!

sleep 5

                                                                               echo "[$(date)] Starting dagster-webserver on 0.0.0.0:$PORT..."
dagster-webserver \
    --host 0.0.0.0 \
    --port "${PORT:-3000}" \
    --workspace workspace.yaml

kill $DAEMON_PID
