# 2023-03-22 14:37

INSTANCE=datasets/backends/sql
DATASET=$INSTANCE
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR

# Run a PostgreSQL instance
docker run --rm --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=secret -d postgres
docker ps

cat ~/.pgpass
cat >> ~/.pgpass <<'EOF'
localhost:5432:postgres:postgres:secret
EOF

export PAGER="less -FSRX"
psql -h localhost -p 5432 -U postgres postgres
\echo :SERVER_VERSION_NAME
#| 15.2 (Debian 15.2-1.pgdg110+1)

CREATE TABLE city (
    id INTEGER PRIMARY KEY,
    code INTEGER,
    name TEXT
);

INSERT INTO city VALUES
    (1, 1, 'Vilnius'),
    (2, 2, 'Kaunas');

CREATE VIEW city_view AS
    SELECT
        code::integer as code,
        name
    FROM city;

SELECT * FROM city_view;
#|  code |  name   
#| ------+---------
#|     1 | Vilnius
#|     2 | Kaunas
\q

# Configure spinta
cat $BASEDIR/config.yml
cat > $BASEDIR/config.yml <<EOF
backends:
  mydb:
    type: sql
    dsn: postgresql://postgres:secret@localhost:5432/postgres
EOF
export SPINTA_CONFIG=$BASEDIR/config.yml

# Create a manifest file
cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref  | source    | access
$DATASET                 |         |      |           |
  | db                   |         | mydb |           |
  |   |   | City         |         | code | city_view |
  |   |   |   | code     | integer |      | code      | open
  |   |   |   | name     | string  |      | name      | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv
poetry run spinta check $BASEDIR/manifest.csv

# Run data publishing service
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Check if it works
http GET "$SERVER/$DATASET/City"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "2c243e9e-dfec-4362-8cfd-82c6953a931c",
#|             "_revision": null,
#|             "_type": "datasets/backends/sql/City",
#|             "code": 1,
#|             "name": "Vilnius"
#|         },
#|         {
#|             "_id": "4c105a26-1a43-4ff6-ac98-c26e9e845bfb",
#|             "_revision": null,
#|             "_type": "datasets/backends/sql/City",
#|             "code": 2,
#|             "name": "Kaunas"
#|         }
#|     ]
#| }


# notes/docker.sh           Start docker compose
# notes/spinta/server.sh    Add client
# notes/postgres.sh         Show tables

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
  mydb:
    type: sql
    dsn: postgresql://postgres:secret@localhost:5432/postgres

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

poetry run spinta show
poetry run spinta wait 1
poetry run spinta bootstrap

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100% 2/2 [00:00<00:00, 81.44it/s]

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type                       _id                                   _revision                             code  name   
#| --------------------------  ------------------------------------  ------------------------------------  ----  -------
#| datasets/backends/sql/City  ebccf7c4-6bfa-4b46-9a1e-8c5302068caf  543f4787-e959-4f99-ae46-0f986006db48  1     Vilnius
#| datasets/backends/sql/City  d0bcae75-4013-4bc2-841a-1c71309bc910  3435e034-d375-4cf6-973c-4b7495d45216  2     Kaunas


test -n "$PID" && kill $PID
docker stop postgres
