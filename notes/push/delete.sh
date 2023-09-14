# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/delete
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country_id   INTEGER,
    FOREIGN KEY  (country_id) REFERENCES countries (id)
);

INSERT INTO countries VALUES (1, 'Lithuania');
INSERT INTO cities VALUES (1, 'Vilnius', 1);
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+-----------+
#| | id |   name    |
#| +----+-----------+
#| | 1  | Lithuania |
#| +----+-----------+
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+------------+
#| | id |  name   | country_id |
#| +----+---------+------------+
#| | 1  | Vilnius | 1          |
#| +----+---------+------------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref     | source       | prepare | level | access
$DATASET                  |         |         |              |         |       |
  | db                    | sql     |         | sqlite:///$BASEDIR/db.sqlite | | |
  |   |   | Country       |         | id      | countries    |         | 4     |
  |   |   |   | id        | integer |         | id           |         | 4     | open
  |   |   |   | name      | string  |         | name         |         | 4     | open
  |   |   | City          |         | id      | cities       |         | 4     |
  |   |   |   | id        | integer |         | id           |         | 4     | open
  |   |   |   | name      | string  |         | name         |         | 4     | open
  |   |   |   | country   | ref     | Country | country_id   |         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/postgres.sh         Show tables

# Run server with internal mode
test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

# Delete all data from internal database, to start with a clean state.
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|####| 2/2 [00:00<00:00, 60.40it/s]

http GET "$SERVER/$DATASET/Country?format(ascii)"
#| _type                _id                                   _revision                             id  name     
#| -------------------  ------------------------------------  ------------------------------------  --  ---------
#| push/delete/Country  14640cd1-78eb-47c6-bb94-82bd446c9823  7767b1e4-715a-4cb7-808c-72d72392698d  1   Lithuania

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type             _id                                   _revision                             id  name     country._id                         
#| ----------------  ------------------------------------  ------------------------------------  --  -------  ------------------------------------
#| push/delete/City  94d5770b-d00a-499b-ba16-46742c83df7a  b6ac10bc-6fc0-46c5-88aa-8e90fe3f83ef  1   Vilnius  14640cd1-78eb-47c6-bb94-82bd446c9823

# Delete all data from source database
sqlite3 $BASEDIR/db.sqlite "DELETE FROM cities;"
sqlite3 $BASEDIR/db.sqlite "DELETE FROM countries;"

# Push again, all data from target should be deleted too.
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 0it [00:00, ?it/s]                                                                                                                                                                                                                                         
#| PUSH DELETED: 100%|####| 2/2 [00:00<00:00, 1415.32it/s]

sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "'$DATASET'/City";'
# (empty table)
sqlite3 $BASEDIR/push/localhost.db 'SELECT * FROM "'$DATASET'/Country";'
# (empty table)

http GET "$SERVER/$DATASET/Country?format(ascii)"
# (empty table)

http GET "$SERVER/$DATASET/City?format(ascii)"
# (empty table)

http GET "$SERVER/$DATASET/City/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name   
#| ------  --------------------------  ------------------------------------  -------
#| insert  2023-07-26T06:24:45.541607  94d5770b-d00a-499b-ba16-46742c83df7a  Vilnius
#| delete  2023-07-26T06:27:52.644064  94d5770b-d00a-499b-ba16-46742c83df7a  ∅

http GET "$SERVER/$DATASET/Country/:changes?select(_op,_created,_id,name)&format(ascii)"
#| _op     _created                    _id                                   name     
#| ------  --------------------------  ------------------------------------  ---------
#| insert  2023-07-26T06:24:45.541607  14640cd1-78eb-47c6-bb94-82bd446c9823  Lithuania
#| delete  2023-07-26T06:27:52.644064  14640cd1-78eb-47c6-bb94-82bd446c9823  ∅
