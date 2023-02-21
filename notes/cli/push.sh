# 2023-02-21 14:23

git log -1 --oneline
#| fe9155c (HEAD -> 311-push-does-not-repeat-errored-chunks, origin/311-push-does-not-repeat-errored-chunks) 311 add repeat count to push command

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/push
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id          INTEGER PRIMARY KEY,
    name        TEXT
);
CREATE TABLE IF NOT EXISTS cities (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    country_id  INTEGER,
    FOREIGN KEY (country_id) REFERENCES countries (id)
);

INSERT INTO countries VALUES (1, 'Lithuania');
INSERT INTO cities VALUES (1, 'Vilnius', 1);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | source     | prepare | access
$DATASET                 |         |         |            |         |
  | db                   | sql     |         | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | Country      |         | id      | countries  |         |
  |   |   |   | id       | integer |         | id         |         | open
  |   |   |   | name     | string  |         | name       |         | open
  |   |   | City         |         | id      | cities     |         |
  |   |   |   | id       | integer |         | id         |         | open
  |   |   |   | name     | string  |         | name       |         | open
  |   |   |   | country  | ref     | Country | country_id |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

kill $(grep -Po 'server process \[\K\d+' $BASEDIR/spinta.log | tail -1)
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &
tail -50 $BASEDIR/spinta.log

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

kill $(grep -Po 'server process \[\K\d+' $BASEDIR/spinta.log | tail -1)
poetry run spinta run &>> $BASEDIR/spinta.log &
tail -50 $BASEDIR/spinta.log

# notes/spinta/push.sh      Configure client

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

sqlite3 $BASEDIR/keymap.db .tables
sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/Country";'
sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/City";'

sqlite3 $BASEDIR/push/localhost.db .tables
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'

sqlite3 $BASEDIR/db.sqlite "DELETE FROM cities WHERE id = 1;"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost

sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/Country";'
sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/City";'

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"

sqlite3 $BASEDIR/db.sqlite "INSERT INTO cities VALUES (2, 'Kaunas', 1);"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost

sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/Country";'
sqlite3 $BASEDIR/keymap.db 'SELECT * FROM "cli/push/City";'

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/City";'
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "cli/push/Country";'

http GET "$SERVER/$DATASET/Country?format(ascii)"
http GET "$SERVER/$DATASET/City?format(ascii)"
