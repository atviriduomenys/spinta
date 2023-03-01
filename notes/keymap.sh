# 2023-03-01 19:44

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=keymap
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id          INTEGER PRIMARY KEY,
    code        TEXT,
    name        TEXT
);
CREATE TABLE IF NOT EXISTS cities (
    name        TEXT PRIMARY KEY,
    country     TEXT,
    FOREIGN KEY (country) REFERENCES countries (code)
);
INSERT INTO countries VALUES (1, 'lt', 'Lithuania');
INSERT INTO cities VALUES ('Vilnius', 'lt');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+------+-----------+
#| | id | code |   name    |
#| +----+------+-----------+
#| | 1  | lt   | Lithuania |
#| +----+------+-----------+
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +---------+---------+
#| |  name   | country |
#| +---------+---------+
#| | Vilnius | lt      |
#| +---------+---------+


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property   | type    | ref           | source    | access
$DATASET                   |         |               |           |
  | db                     | sql     |               | sqlite:///$BASEDIR/db.sqlite |
  |   |   | Country        |         | id            | countries |
  |   |   |   | id         | integer |               | id        | open
  |   |   |   | code       | string  |               | code      | open
  |   |   |   | name       | string  |               | name      | open
  |   |   | City           |         | name          | cities    |
  |   |   |   | name       | string  |               | name      | open
  |   |   |   | country    | ref     | Country[code] | country   | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show


test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/server.sh    Add client
# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Country?select(_id)&format(ascii)"
#| ------------------------------------
#| _id
#| f52eb124-9e7d-48b2-81f5-06f9f048087b
#| ------------------------------------

http GET "$SERVER/$DATASET/City?select(country)&format(ascii)"
#| ------------------------------------
#| country._id
#| 6ce3236d-7281-4833-865f-617a0085b05f
#| ------------------------------------
# FIXME: Country._id and City.country._id do not match.
