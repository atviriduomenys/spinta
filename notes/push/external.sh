# 2023-06-20 07:22

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/external
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country_id   INTEGER
);

INSERT INTO cities VALUES (1, 'Vilnius', 10);
INSERT INTO cities VALUES (2, 'Ryga', 20);
INSERT INTO cities VALUES (3, 'Tallin', 30);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | source     | level | access
$DATASET/internal        |         |         |            |       |
  | db                   | sql     |         | sqlite:///$BASEDIR/db.sqlite | |
  |   |   | City         |         | id      | cities     | 4     |
  |   |   |   | id       | integer |         | id         | 4     | open
  |   |   |   | name     | string  |         | name       | 4     | open
  |   |   |   | country  | ref     | /$DATASET/external/Country | country_id | 4 | open
$DATASET/external        |         |         |            |       |
  |   |   | Country      |         | id      |            | 4     |
  |   |   |   | id       | integer |         |            | 4     | open
  |   |   |   | name     | string  |         |            | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/external/Country?count()"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "ProgrammingError",
#|             "message": "
#|                 (psycopg2.errors.UndefinedTable) relation
#|                     "push/external/external/Country"
#|                 does not exist
#|                 LINE 2:
#|                     FROM "push/external/external/Country"
#|                                  ^
#|                 SQL:
#|                     SELECT count(*) AS "count()"
#|                     FROM "push/external/external/Country"
#|             "
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

http DELETE "$SERVER/$DATASET/internal/:wipe" $AUTH

http GET "$SERVER/$DATASET/internal/City?count()"
http GET "$SERVER/$DATASET/external/Country?count()"

http POST $SERVER/$DATASET/external/Country $AUTH id:=10 name=Lithuania
http POST $SERVER/$DATASET/external/Country $AUTH id:=20 name=Latvia
http POST $SERVER/$DATASET/external/Country $AUTH id:=30 name=Estonia

http GET "$SERVER/$DATASET/external/Country?format(ascii)"

rm $BASEDIR/keymap.db
poetry run python - sqlite:///$PWD/$BASEDIR/keymap.db $SERVER $AUTH $DATASET
import sys
dsn, server, auth, dataset = sys.argv[1:]
import sqlalchemy as sa
engine = sa.create_engine(dsn)
metadata = sa.MetaData(engine)
name = f'{dataset}/external/Country'
table = sa.Table(
    name, metadata,
    sa.Column('key', sa.Text, primary_key=True),
    sa.Column('hash', sa.Text, unique=True),
    sa.Column('value', sa.LargeBinary),
)
table.create(checkfirst=True)
conn = engine.connect()
import requests
resp = requests.get(f'http://localhost{server}/{dataset}/external/Country', headers={
    'Authorization': auth.split(':', 1)[1].strip(),
})
data = resp.json()
import msgpack, hashlib
for row in data['_data']:
    key = row['_id']
    value = row['id']
    value = msgpack.dumps(value, strict_types=True)
    hash = hashlib.sha1(value).hexdigest()
    conn.execute(table.insert(), {
        'key': key,
        'hash': hash,
        'value': value,
    })

conn.close()
exit()
sqlite3 $BASEDIR/keymap.db 'SELECT *, hex(value) FROM "'$DATASET'/external/Country";'
http GET "$SERVER/$DATASET/external/Country?format(ascii)"

rm $BASEDIR/push/localhost.db
poetry run spinta push $BASEDIR/manifest.csv -d $DATASET/internal -o test@localhost

http GET "$SERVER/$DATASET/internal/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| Loading InlineManifest manifest default
#| Get access token from http://localhost:8000
#| PUSH:   2%|#9                                                                                                                            | 15154/1000000 [01:56<2:03:14, 133.19it/s]
#| push/pbar/Data:   2%|#7 | 15155/1000000 [01:56<2:03:05, 133.35it/s]

poetry run spinta push --no-progress-bar $BASEDIR/manifest.csv -o test@localhost
#| Loading InlineManifest manifest default
#| Get access token from http://localhost:8000
