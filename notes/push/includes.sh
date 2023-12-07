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
#| push/includes/City  1cacb9c9-4350-4df2-8b5b-678c5669d731  ∅          ∅      1   Vilnius  lt

http GET "$SERVER/$DATASET/countries/Country?format(ascii)"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "BackendNotGiven",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "push/includes/countries",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "push/includes/countries/Country",
#|                 "schema": "11"
#|             },
#|             "message": "Model is operating in external mode, yet it does not have assigned backend to it.",
#|             "template": "Model is operating in external mode, yet it does not have assigned backend to it.",
#|             "type": "model"
#|         }
#|     ]
#| }

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
http GET "$SERVER/$DATASET/countries/Country?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|###| 1/1 [00:00<00:00, 167.01it/s]

http GET "$SERVER/$DATASET/City?limit(5)&format(ascii)"
#| id  name     country.code
#| --  -------  ------------
#| 1   Vilnius  lt
http GET "$SERVER/$DATASET/countries/Country?format(ascii)"
# (no data)
