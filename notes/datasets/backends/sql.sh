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


test -n "$PID" && kill $PID
docker stop postgres
