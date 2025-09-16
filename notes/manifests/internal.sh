# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=manifests/internal
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE

mkdir -p $BASEDIR

export SPINTA_CONFIG=$BASEDIR/config.yml

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
    type: internal
    path: postgresql://admin:admin123@localhost:54321/spinta
    backend: default
    keymap: default
    mode: internal

accesslog:
  type: file
  file: $PWD/$BASEDIR/accesslog.json
EOF

poetry run spinta --tb=native bootstrap
#| Traceback (most recent call last):
#|   File "spinta/cli/migrate.py", line 27, in bootstrap
#|     store = prepare_manifest(context, ensure_config_dir=True)
#|   File "spinta/cli/helpers/store.py", line 132, in prepare_manifest
#|     store = load_manifest(
#|   File "spinta/cli/helpers/store.py", line 116, in load_manifest
#|     commands.load(
#|   File "spinta/manifests/internal_sql/commands/load.py", line 49, in load
#|     load_manifest_nodes(context, manifest, schemas)
#|   File "spinta/manifests/helpers.py", line 134, in load_manifest_nodes
#|     for eid, schema in schemas:
#|   File "spinta/manifests/internal_sql/helpers.py", line 36, in read_schema
#|     yield from _read_all_sql_manifest_rows(path, conn)
#|   File "spinta/manifests/internal_sql/helpers.py", line 75, in _read_all_sql_manifest_rows
#|     rows = conn.execute(stmt)
#| ProgrammingError: (psycopg2.errors.UndefinedTable) relation "_manifest" does not exist
# FIXME: `spinta bootstrap` should create _manifest table.

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref     | access
$DATASET                    |         |         |
  |   |   | Country         |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
  |   |   |   |             |         |         |
  |   |   | City            |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
  |   |   |   | country     | ref     | Country | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o postgresql://admin:admin123@localhost:54321/spinta
poetry run spinta show

psql -h localhost -p 54321 -U admin spinta -c 'select * from _manifest;'

poetry run spinta bootstrap

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client
