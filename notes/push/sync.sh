# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/sync
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+
#| | id |  name   |
#| +----+---------+
#| | 1  | Vilnius |
#| +----+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref                          | source       | prepare | level | access
$DATASET                  |         |                              |              |         |       |
  | db                    | sql     |                              | sqlite:///$BASEDIR/db.sqlite | | |
  |   |   | City          |         | id                           | cities       |         | 4     |
  |   |   |   | id        | integer |                              | id           |         | 4     | open
  |   |   |   | name      | string  |                              | name         |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

uuidgen
VILNIUS=646d0f3f-1d6d-493e-b1b9-b6d1472d36fb
http POST "$SERVER/$DATASET/City" $AUTH _id=$VILNIUS id:=1 name=Vilnius
KAUNAS=c4c53887-9332-4643-8060-d231ddf76c1d
http POST "$SERVER/$DATASET/City" $AUTH _id=$KAUNAS id:=2 name=Kaunas

http GET "$SERVER/$DATASET/City?format(ascii)"

rm $BASEDIR/keymap.db
rm $BASEDIR/push/localhost.db
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost --sync
#| {
#|     '_id': '85828ce4-5264-4fc0-bd9a-699ee91f6298',
#|     '_op': 'insert',
#|     '_type': 'push/sync/City',
#|     'id': 1,
#|     'name': 'Vilnius',
#| }
#| Server response (status=400):
#|     {
#|         'errors': [
#|             {
#|                 'code': 'UniqueConstraint',
#|                 'context': {
#|                     'attribute': 'id',
#|                     'component': 'spinta.components.Property',
#|                     'dataset': 'push/sync',
#|                     'entity': 'cities',
#|                     'manifest': 'default',
#|                     'model': 'push/sync/City',
#|                     'property': 'id',
#|                     'resource': 'db',
#|                     'resource.backend': 'push/sync/db',
#|                     'schema': '5',
#|                 },
#|                 'message': 'Given value already exists.',
#|                 'template': 'Given value already exists.',
#|                 'type': 'property',
#|             },
#|         ],
#|     }
# FIXME: It looks, that sync was not done, because an attempt to create an
#        existing City was made.

sqlite3 $BASEDIR/push/localhost.db "SELECT * FROM '"$DATASET"/City'"
#| +--------------------------------------+------------------------------------------+----------+----------------------------+-------+--------------------------------------------------------------+-------------+
#| |                  id                  |                 checksum                 | revision |           pushed           | error |                             data                             | synchronize |
#| +--------------------------------------+------------------------------------------+----------+----------------------------+-------+--------------------------------------------------------------+-------------+
#| | 85828ce4-5264-4fc0-bd9a-699ee91f6298 | 866314955098a3fa463e7ad8e224437bb6860094 |          | 2023-08-04 15:24:01.083430 | 1     | {"_type": "push/sync/City", "_id": "85828ce4-5264-4fc0-bd9a- |             |
#| |                                      |                                          |          |                            |       | 699ee91f6298", "id": 1, "name": "Vilnius", "_op": "insert"}  |             |
#| +--------------------------------------+------------------------------------------+----------+----------------------------+-------+--------------------------------------------------------------+-------------+
# FIXME: Yes, an erros was registerend and nothing was pushed. Instead of using VILNIUS=646d0f3f-1d6d-493e-b1b9-b6d1472d36fb, a new id was generated.

sqlite3 $BASEDIR/keymap.db "SELECT * FROM '"$DATASET"/City'"
#| +--------------------------------------+------------------------------------------+-------+
#| |                 key                  |                   hash                   | value |
#| +--------------------------------------+------------------------------------------+-------+
#| | 85828ce4-5264-4fc0-bd9a-699ee91f6298 | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       |
#| +--------------------------------------+------------------------------------------+-------+
# FIXME: It looks, that a new id was generated, instead of using existing VILNIUS=646d0f3f-1d6d-493e-b1b9-b6d1472d36fb.
