# 2023-03-02 15:33

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/pbar
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS data (
    id          INTEGER PRIMARY KEY,
    number      INTEGER
);

INSERT INTO data
    WITH RECURSIVE gendata(num) AS (
        SELECT random()
        UNION ALL
        SELECT random() FROM gendata
        LIMIT 1000000
    )
    SELECT NULL, num FROM gendata;
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM data LIMIT 1;"
#| +----+--------------------+
#| | id |       number       |
#| +----+--------------------+
#| | 1  | 387528935542014284 |
#| +----+--------------------+
sqlite3 $BASEDIR/db.sqlite "SELECT COUNT(*) FROM data;"
#| +------------+
#| | COUNT(*)   |
#| +------------+
#| | 1,000,000  |
#| +------------+
du -sh $BASEDIR/db.sqlite
#| 17M     var/instances/push/pbar/db.sqlite

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref | source | access
$DATASET                 |         |     |        |
  | db                   | sql     |     | sqlite:///$BASEDIR/db.sqlite |
  |   |   | Data         |         | id  | data   |
  |   |   |   | id       | integer |     | id     | open
  |   |   |   | number   | integer |     | number | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Data?limit(5)&format(ascii)"
#| --------------  ------------------------------------  ---------  --  --------------------
#| _type           _id                                   _revision  id  number
#| push/pbar/Data  b99d9af4-1b32-47a5-8692-4e120429a972  ∅          1   387528935542014284
#| push/pbar/Data  07746e86-47a0-42ae-9f52-9f97147cdcc3  ∅          2   -2468068816092021607
#| push/pbar/Data  d877656b-3710-4268-8787-33d7a3eab0e6  ∅          3   8144652870968327750
#| push/pbar/Data  76a21ff4-fc63-43d8-b4c6-c3cf0c7c6c9b  ∅          4   -8389724124755352880
#| push/pbar/Data  97c88e08-8f33-4006-bd2e-44e35cd40256  ∅          5   8488930546184150651
#| --------------  ------------------------------------  ---------  --  --------------------

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

rm $BASEDIR/push/localhost.db
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/Data?limit(5)&format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| Loading InlineManifest manifest default
#| Get access token from http://localhost:8000
#| PUSH:   2%|#9                                                                                                                            | 15154/1000000 [01:56<2:03:14, 133.19it/s]
#| push/pbar/Data:   2%|#7 | 15155/1000000 [01:56<2:03:05, 133.35it/s]

poetry run spinta push --no-progress-bar $BASEDIR/manifest.csv -o test@localhost
#| Loading InlineManifest manifest default
#| Get access token from http://localhost:8000
