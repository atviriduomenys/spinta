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

export SECRET=$(head /dev/urandom | tr -dc A-Za-z0-9 | head -c 16)
echo "##############################"
echo "SECRET:"
echo $SECRET

# Ensure client folders exists
poetry run spinta upgrade clients

git clone https://github.com/atviriduomenys/manifest.git
git clone https://github.com/atviriduomenys/demo-saltiniai.git

mkdir manifests

find demo-saltiniai/manifest -name "*.csv" | xargs -I{} mv "{}" manifests/
cat manifest/get_data_gov_lt.in | xargs -I{} mv "manifest/{}" manifests/

ls manifests/* | xargs poetry run spinta copy -o manifest.csv
cp manifest/datasets/gov/vssa/demo/sql/sql_sdsa.csv sql_sdsa.csv

rm -rf manifest manifests demo-saltiniai

cat >> ~/.config/spinta/credentials.cfg <<EOF
[test@localhost]
client = test
secret = $SECRET
server = http://localhost:8000
scopes =
  uapi:/:set_meta_fields
  uapi:/:auth_clients
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:changes
  uapi:/:create
  uapi:/:update
  uapi:/:patch
  uapi:/:delete
  uapi:/:wipe
EOF

#export PGPASSWORD=admin123
#cat database.psql | psql -H $DB_HOST_INTERNAL -U admin spinta

poetry run spinta upgrade
poetry run spinta migrate -p
poetry run spinta migrate

sleep 10 && poetry run spinta push sql_sdsa.csv -o test@localhost &
make run
