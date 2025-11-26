#!/bin/bash
set -x

if [ "${SPINTA_INTERNAL}" = 'true' ]; then
    bash ./entrypoint-internal.sh
    exit
fi

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
    mode: external
accesslog:
  type: file
  file: stdout
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

poetry run spinta upgrade clients

git clone https://github.com/atviriduomenys/demo-saltiniai.git
mkdir manifests
find demo-saltiniai/manifest -name "*.csv" | xargs -I{} mv "{}" manifests/
rm -f manifests/kdsa.csv
ls manifests/* | xargs poetry run spinta copy -o manifest.csv

rm -rf demo-saltiniai manifests

poetry run spinta upgrade
make run
