# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=datasets/sql/getone
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property     | type    | ref | level | source  | access
$DATASET                     |         |     |       |         |
  | db                       | sql     |     |       | sqlite:///$BASEDIR/db.sqlite |
  |   |   | City             |         | id  |       | cities  |
  |   |   |   | id           | integer |     | 4     | id      | open
  |   |   |   | name         | string  |     | 4     | name    | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);

INSERT INTO cities VALUES (1, 'Vilnius');
INSERT INTO cities VALUES (2, 'Kaunas');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _id                                   _revision  _page  id  name   
#| ------------------------------------  ---------  -----  --  -------
#| 47cb18cb-77e7-4216-852d-122c9214a3af  ∅          WzFd   1   Vilnius
#| d8444e1b-912d-470d-bf34-8306637854a5  ∅          WzJd   2   Kaunas

http GET "$SERVER/$DATASET/City/47cb18cb-77e7-4216-852d-122c9214a3af"

#{
#    "_id": "47cb18cb-77e7-4216-852d-122c9214a3af",
#    "_type": "datasets/sql/getone/City",
#    "id": 1,
#    "name": "Vilnius"
#}
