#!/bin/bash
set -x

export SPINTA_CONFIG=$PWD/config.yml
if [ ! -f "$SPINTA_CONFIG" ]; then
    cat > "$SPINTA_CONFIG" <<EOF
data_path: $PWD/$BASEDIR
default_auth_client: default
keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$PWD/$BASEDIR/keymap.db
backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@${DB_HOST:=localhost}:${DB_PORT:=5432}/spinta
manifest: default
manifests:
  default:
    type: csv
    path: /app/manifest.csv
    backend: default
    keymap: default
    mode: internal
accesslog:
  type: file
  file: stdout
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

# Ensure client folders exists
poetry run spinta upgrade clients

git clone https://github.com/atviriduomenys/demo-saltiniai.git
mkdir manifests
find demo-saltiniai/manifest -name "*.csv" | xargs -I{} mv "{}" manifests/
rm -f manifests/kdsa.csv
ls manifests/* | xargs poetry run spinta copy -o manifest.csv

rm -rf demo-saltiniai manifests

poetry run spinta bootstrap

CLIENTS_FOLDER="${XDG_CONFIG_HOME:-$HOME}/.config/spinta/clients"
if [ ! -d $CLIENTS_FOLDER ] ||
   [ ! -f "$CLIENTS_FOLDER/helpers/keymap.yml" ] ||
   ! grep -q '^client:' "$CLIENTS_FOLDER/helpers/keymap.yml"; then
  poetry run spinta client add -n client -s secret --scope - <<EOF
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

poetry run spinta upgrade
make run
