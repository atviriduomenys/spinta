#!/bin/bash


CONFIG_FILE="config.yml"

if [ ! -f "$CONFIG_FILE" ]; then
    export SPINTA_CONFIG=config.yml
    cat > "$CONFIG_FILE" <<EOF
env: test
data_path: $PWD/$BASEDIR
default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$PWD/$BASEDIR/keymap.db

backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@${DB_HOST:=localhost}:${DB_PORT:=54321}/spinta

manifest: default
manifests:
  default:
    type: csv
    path: $PWD/$BASEDIR/manifest.csv
    backend: default
    keymap: default
    mode: external

accesslog:
  type: file
  file: stdout
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

echo "Touch manifest.csv"
touch manifest.csv

poetry install spinta
spinta upgrade
make run
