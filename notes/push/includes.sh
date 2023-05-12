# 2023-05-12 13:28

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/includes
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE cities (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    country     TEXT   
);
INSERT INTO cities VALUES (1, 'Vilnius', 'lt');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities"
#| +----+---------+---------+
#| | id |  name   | country |
#| +----+---------+---------+
#| | 1  | Vilnius | lt      |
#| +----+---------+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref                         | source   | level | access
$DATASET                 |         |                             |          |       |
  | db                   | sql     |                             | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | City         |         | id                          | cities   | 4     |
  |   |   |   | id       | integer |                             | id       | 4     | open
  |   |   |   | name     | string  |                             | name     | 2     | open
  |   |   |   | country  | ref     | /$DATASET/countries/Country | country  | 3     | open
  |   |   |   |          |         |                             |          |       |
$DATASET/countries       |         |                             |          |       |
  |   |   | Country      |         | code                        |          | 4     |
  |   |   |   | code     | string  |                             |          | 4     | open
  |   |   |   | name     | string  |                             |          | 2     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/City?limit(5)&format(ascii)"
#| _type               _id                                   _revision  _base  id  name     country.code
#| ------------------  ------------------------------------  ---------  -----  --  -------  ------------
#| push/includes/City  1cacb9c9-4350-4df2-8b5b-678c5669d731  ∅          ∅      1   Vilnius  ∅
# TODO: country.code should not be null.

# notes/spinta/server.sh    Run migrations

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

rm $BASEDIR/push/localhost.db
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/City?limit(5)&format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| 2023-05-12 13:56:41,105 ERROR: Error when sending and receiving data. Model push/includes/City, data:                                                                     
#|  {
#|     '_id': '1cacb9c9-4350-4df2-8b5b-678c5669d731',
#|     '_op': 'insert',
#|     '_type': 'push/includes/City',
#|     'country': {'_id': '6fbef53b-efa6-4c8c-b13c-37e78197e089'},
# FIXME: This should be: 'country': {'code': '6fbef53b-efa6-4c8c-b13c-37e78197e089'}
#        See: https://github.com/atviriduomenys/spinta/issues/208
#             https://github.com/atviriduomenys/spinta/blob/66d8696ce5a889ceaad9ad9fe4b8def5e6f16ad8/notes/types/ref/external.sh#L111
#|     'id': 1,
#|     'name': 'Vilnius',
#| }
#| Server response (status=400):
#|     {
#|         'errors': [
#|             {
#|                 'code': 'FieldNotInResource',
#|                 'context': {
#|                     'attribute': 'country',
#|                     'component': 'spinta.components.Property',
#|                     'dataset': 'push/includes',
#|                     'entity': 'cities',
#|                     'id': '1cacb9c9-4350-4df2-8b5b-678c5669d731',
#|                     'manifest': 'default',
#|                     'model': 'push/includes/City',
#|                     'property': '_id',
#|                     'resource': 'db',
#|                     'resource.backend': 'push/includes/db',
#|                     'schema': '5',
#|                 },
#|                 'message': "Unknown property '_id'.",
#|                 'template': 'Unknown property {property!r}.',
#|                 'type': 'property',
#|             },
#|         ],
#|     }
