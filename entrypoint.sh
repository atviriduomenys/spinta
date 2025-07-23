#!/bin/bash


CONFIG_FILE="config.yml"

if [ ! -f "$CONFIG_FILE" ]; then
    export SPINTA_CONFIG=config.yml
    cat > "$CONFIG_FILE" <<EOF
env: test
data_path: $PWD/$BASEDIR
default_auth_client: default

backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@${DB_HOST:=localhost}:${DB_PORT:=54321}/spinta

accesslog:
  type: file
  file: stdout
EOF
    echo "Created config.yml."
else
    echo "Found existing config.yml."
fi

git clone https://github.com/atviriduomenys/demo-saltiniai.git
mkdir manifests
find demo-saltiniai/manifest -name "*.csv" | xargs -I{} mv "{}" manifests/
spinta upgrade
ls manifests | xargs spinta copy -o manifest.csv

rm -rf demo-saltiniai manifests

poetry install spinta[all]
spinta upgrade
make run
