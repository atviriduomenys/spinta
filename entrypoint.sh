#!/bin/bash
CONFIG_FILE="config.yml"
SPINTA_MODE=${SPINTA_MODE:=external}

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
  external:
    type: sql/postgresql
    dsn: postgresql://admin:admin123@${DB_HOST:=localhost}:${DB_PORT:=54321}/spinta
  internal:
    type: postgresql
    dsn: postgresql://admin:admin123@${INTERNAL_DB_HOST:=localhost}:${INTERNAL_DB_PORT:=5432}/spinta
manifest: default
manifests:
  default:
    type: csv
    path: /app/manifest.csv
    backend: $SPINTA_MODE
    keymap: default
    mode: $SPINTA_MODE
accesslog:
  type: file
  file: stdout
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

if [ "$SPINTA_MODE" = "external" ]; then
  git clone https://github.com/atviriduomenys/demo-saltiniai.git
  mkdir manifests
  find demo-saltiniai/manifest -name "*.csv" | xargs -I{} mv "{}" manifests/
  rm -f manifests/kdsa.csv
  ls manifests/* | xargs poetry run spinta copy -o manifest.csv

  rm -rf demo-saltiniai manifests
else
  git clone https://github.com/atviriduomenys/manifest.git
  (
    cd manifest
    git checkout get.data.gov.lt
    git pull
    cat get_data_gov_lt.in | xargs spinta copy -o ../manifest.csv
  )
  rm -rf manifest

  poetry run spinta bootstrap

  if [ ! -d ~/.config/spinta/clients ]; then
    poetry run spinta client add -n default -s secret --scope - <<EOF
spinta_set_meta_fields
spinta_auth_clients
spinta_getone
spinta_getall
spinta_search
spinta_changes
spinta_insert
spinta_upsert
spinta_update
spinta_patch
spinta_delete
spinta_wipe
EOF
  fi
fi

poetry run spinta upgrade
poetry run spinta admin model_limit
make run
