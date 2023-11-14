# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/base
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref  | source                       | access
$DATASET                   |         |      |                              |
  | db                     | sql     |      | sqlite:///$BASEDIR/db.sqlite |
  |   |   |   |            |         |      |                              |     
  |   |   | Location       |         | id   | locations                    |
  |   |   |   | id         | integer |      | id                           | open
  |   |   |   | name       | string  |      | name                         | open
  |   |   |   | population | integer |      | population                   | open
  |   |   |   |            |         |      |                              |     
  |   | Location           |         | name |                              |
  |   |   | City           |         | id   | cities                       |
  |   |   |   | id         | integer |      | id                           | open
  |   |   |   | name       |         |      | name                         | open
  |   |   |   | population | integer |      | population                   | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS locations (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    population   INTEGER
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    population   INTEGER
);

INSERT INTO locations VALUES (1, 'Vilnius', 42);
INSERT INTO cities VALUES (10, 'Vilnius', 24);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM locations;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
HTTP/1.1 200 OK

{
    "wiped": true
}
test -e $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|######| 2/2 [00:00<00:00, 68.70it/s]

http GET "$SERVER/$DATASET/Location?format(ascii)"
#| _id                                   id  name     population
#| ------------------------------------  --  -------  ----------
#| d35edc8f-59d2-40e2-af6a-add3d23dc86e  1   Vilnius  42

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _id                                   id  name     population
#| ------------------------------------  --  -------  ----------
#| d35edc8f-59d2-40e2-af6a-add3d23dc86e  10  Vilnius  24
