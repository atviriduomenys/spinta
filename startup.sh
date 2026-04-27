#!/bin/bash
set -x

if [ -z "$(ls -A $HOME/config 2>/dev/null)" ]; then
    spinta check
fi

if [ "${SPINTA_AUTO_UPGRADE:-false}" = "true" ]; then
  spinta upgrade
fi

uvicorn spinta.asgi:app --log-level info --host 0.0.0.0
