#!/bin/bash


CONFIG_FILE="config.yml"

if [ ! -f "$CONFIG_FILE" ]; then
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
    dsn: postgresql://admin:admin123@localhost:54321/spinta

manifest: default
manifests:
  default:
    type: csv
    path: $PWD/$BASEDIR/manifest.csv
    backend: default
    keymap: default
    mode: internal

accesslog:
  type: file
  file: $PWD/$BASEDIR/accesslog.json
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

echo "Touch manifest.csv"
touch manifest.csv

poetry install spinta

make run
