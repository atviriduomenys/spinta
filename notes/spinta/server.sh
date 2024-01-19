# Configure server
INSTANCE=test
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR

cat $BASEDIR/config.yml
cat > $BASEDIR/config.yml <<EOF
env: production
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

tree ~/.config/spinta/
poetry run spinta genkeys


# Check configuration
export SPINTA_CONFIG=$BASEDIR/config.yml
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show
poetry run spinta config backends manifests
poetry run spinta show
poetry run spinta check
poetry run spinta wait 1


# Run migrations
poetry run spinta bootstrap
# notes/postgres.sh         Show tables


# Add default client
cat ~/.config/spinta/clients/test.yml
poetry run spinta client add -n default --add-secret --scope - <<EOF
spinta_getone
spinta_getall
spinta_search
spinta_changes
EOF


# Add client
cat ~/.config/spinta/clients/test.yml
poetry run spinta client add -n test -s secret --add-secret --scope - <<EOF
spinta_set_meta_fields
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
spinta_auth_clients
EOF


# Run server

poetry run spinta run &>> $BASEDIR/spinta.log &
PID=$!
tail -50 $BASEDIR/spinta.log
kill $(grep -Po 'server process \[\K\d+' $BASEDIR/spinta.log | tail -1)
kill $PID


# Check if server works
http :8000
http :8000/version


