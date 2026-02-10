#!/bin/bash
set -x

if [ -z "$(ls -A $HOME/config 2>/dev/null)" ]; then
    poetry run spinta check
fi

if [ "${SPINTA_AUTO_UPGRADE:-false}" = "true" ]; then
  poetry run spinta upgrade
fi

poetry run uvicorn spinta.asgi:app --log-level info --host 0.0.0.0
