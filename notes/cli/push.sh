# 2023-02-22 13:04 Paginated push

git log -1 --oneline
#| acb9f1d (HEAD -> 366-paginated-push, origin/master, origin/HEAD, origin/366-paginated-push, master) Merge pull request #333 from atviriduomenys/308-add-export-to-rdf-support

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/push
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id          INTEGER PRIMARY KEY,
    name        TEXT,
    created     TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius', '2023-02-22 13:14:00');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type     | ref   | source  | prepare       | access
$DATASET                 |          |       |         |               |
  | db                   | sql      |       | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | City         |          | id    | cities  | page(created) |
  |   |   |   | id       | integer  |       | id      |               | open
  |   |   |   | name     | string   |       | name    |               | open
  |   |   |   | created  | datetime |       | created |               | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client
