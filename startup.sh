#!/bin/bash

if [ "${SPINTA_AUTO_UPGRADE:-false}" = "true" ]; then
  poetry run spinta upgrade
fi

poetry run uvicorn spinta.asgi:app --reload --log-level info --host 0.0.0.0
