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
    name         TEXT,
    country      TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius', 'lt');
INSERT INTO cities VALUES (2, 'Ryga', 'lv');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+---------+
#| | id |  name   | country |
#| +----+---------+---------+
#| | 1  | Vilnius | lt      |
#| | 2  | Ryga    | lv      |
#| +----+---------+---------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref                              | source                       | level | access
$DATASET/external        |         |                                  |                              |       |
  |   |   | Country      |         | id                               |                              | 4     |
  |   |   |   | id       | integer |                                  |                              | 4     | open
  |   |   |   | code     | string  |                                  |                              | 4     | open
  |   |   |   | name     | string  |                                  |                              | 4     | open
$DATASET                 |         |                                  |                              |       |
  | db                   | sql     |                                  | sqlite:///$BASEDIR/db.sqlite |       |
  |   |   | City         |         | id                               | cities                       | 4     |
  |   |   |   | id       | integer |                                  | id                           | 4     | open
  |   |   |   | name     | string  |                                  | name                         | 4     | open
  |   |   |   | country  | ref     | /$DATASET/external/Country[code] | country                      | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

LT=646d0f3f-1d6d-493e-b1b9-b6d1472d36fb
http POST "$SERVER/$DATASET/external/Country" $AUTH _id=$LT id:=1 code=lt name=Lithuania

LV=c4c53887-9332-4643-8060-d231ddf76c1d
http POST "$SERVER/$DATASET/external/Country" $AUTH _id=$LV id:=2 code=lv name=Latvia

http GET "$SERVER/$DATASET/external/Country?format(ascii)"
#| _id                                   id  code  name     
#| ------------------------------------  --  ----  ---------
#| 646d0f3f-1d6d-493e-b1b9-b6d1472d36fb  1   lt    Lithuania
#| c4c53887-9332-4643-8060-d231ddf76c1d  2   lv    Latvia

test -f $BASEDIR/keymap.db && rm $BASEDIR/keymap.db
test -f $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db
poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| push/sync/City: 0it [00:00, ?it/s]?it/s]
#| push/sync/external/Country: 2it [00:00, 22.84it/s]
#| push/sync/external/Country.code: 2it [00:00, 22.83it/s]
#| SYNCHRONIZING KEYMAP: 4it [00:00, 38.81it/s] ?it/s]
#| PUSH: 100%|###| 2/2 [00:00<00:00, 54.64it/s]

sqlite3 $BASEDIR/keymap.db ".tables"
#| _synchronize                     push/sync/external/Country     
#| push/sync/City                   push/sync/external/Country.code

sqlite3 $BASEDIR/keymap.db "SELECT * FROM '_synchronize'"
#| +---------------------------------+-----+----------------------------+
#| |              model              | cid |          updated           |
#| +---------------------------------+-----+----------------------------+
#| | push/sync/City                  | 0   | 2023-09-30 09:11:01.167717 |
#| | push/sync/external/Country      | 2   | 2023-09-30 09:11:01.251647 |
#| | push/sync/external/Country.code | 2   | 2023-09-30 09:11:01.259414 |
#| +---------------------------------+-----+----------------------------+

sqlite3 $BASEDIR/keymap.db "SELECT *, hex(value) FROM '"$DATASET"/external/Country'"
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 646d0f3f-1d6d-493e-b1b9-b6d1472d36fb | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| | c4c53887-9332-4643-8060-d231ddf76c1d | c4ea21bb365bbeeaf5f2c654883e56d11e43c44e |       | 02         |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/keymap.db "SELECT *, hex(value) FROM '"$DATASET"/external/Country.code'"
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | 646d0f3f-1d6d-493e-b1b9-b6d1472d36fb | 7b14047ce12579e28dae962a25b010b3ace48367 | �lt    | A26C74     |
#| | c4c53887-9332-4643-8060-d231ddf76c1d | 8870635ffb9f59e9dbb04fb1fd711806c5d2741c | �lv    | A26C76     |
#| +--------------------------------------+------------------------------------------+-------+------------+

sqlite3 $BASEDIR/keymap.db "SELECT *, hex(value) FROM '"$DATASET"/City'"
#| +--------------------------------------+------------------------------------------+-------+------------+
#| |                 key                  |                   hash                   | value | hex(value) |
#| +--------------------------------------+------------------------------------------+-------+------------+
#| | dd933591-fa50-49b9-863d-1eccc46554ec | bf8b4530d8d246dd74ac53a13471bba17941dff7 |       | 01         |
#| | 7194b728-4077-41e3-9f83-c07944e663d7 | c4ea21bb365bbeeaf5f2c654883e56d11e43c44e |       | 02         |
#| +--------------------------------------+------------------------------------------+-------+------------+

http GET "$SERVER/$DATASET/external/Country?format(ascii)"
#| _id                                   id  code  name     
#| ------------------------------------  --  ----  ---------
#| 646d0f3f-1d6d-493e-b1b9-b6d1472d36fb  1   lt    Lithuania
#| c4c53887-9332-4643-8060-d231ddf76c1d  2   lv    Latvia

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _id                                   id  name     country._id                         
#| ------------------------------------  --  -------  ------------------------------------
#| dd933591-fa50-49b9-863d-1eccc46554ec  1   Vilnius  646d0f3f-1d6d-493e-b1b9-b6d1472d36fb
#| 7194b728-4077-41e3-9f83-c07944e663d7  2   Ryga     c4c53887-9332-4643-8060-d231ddf76c1d

http GET "$SERVER/$DATASET/City?select(country.code, country.name, name)&format(ascii)"
#| country.code  country.name  name   
#| ------------  ------------  -------
#| lt            Lithuania     Vilnius
#| lv            Latvia        Ryga


sqlite3 $BASEDIR/db.sqlite "INSERT INTO cities VALUES (3, 'Tallinn', 'ee');"
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+---------+---------+
#| | id |  name   | country |
#| +----+---------+---------+
#| | 1  | Vilnius | lt      |
#| | 2  | Ryga    | lv      |
#| | 3  | Tallinn | ee      |
#| +----+---------+---------+

EE=8b164c72-10cb-41c8-bdb2-4c291aac80a7
http POST "$SERVER/$DATASET/external/Country" $AUTH _id=$EE id:=3 code=ee name=Estonia

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| push/sync/external/Country: 1it [00:00, 26.58it/s]
#| push/sync/external/Country.code: 1it [00:00, 26.58it/s]
#| SYNCHRONIZING KEYMAP: 2it [00:00, 46.89it/s] ?it/s]
#| PUSH: 100%|######| 3/3 [00:00<00:00, 183.40it/s]

http GET "$SERVER/$DATASET/City?select(country.code, country.name, name)&format(ascii)"
#| country.code  country.name  name   
#| ------------  ------------  -------
#| lt            Lithuania     Vilnius
#| lv            Latvia        Ryga
#| ee            Estonia       Tallinn


poetry run spinta push --sync $BASEDIR/manifest.csv -o test@localhost
#| push/sync/City: 3it [00:00, 80.73it/s]s]
#| push/sync/external/Country: 3it [00:00, 47.03it/s]
#| push/sync/external/Country.code: 3it [00:00, 47.00it/s]
#| SYNCHRONIZING KEYMAP: 9it [00:00, 82.55it/s] ?it/s]
#| PUSH: 100%|######| 3/3 [00:00<00:00, 594.60it/s]


http GET "$SERVER/$DATASET/City?select(country.code, country.name, name)&format(ascii)"
#| country.code  country.name  name   
#| ------------  ------------  -------
#| lt            Lithuania     Vilnius
#| lv            Latvia        Ryga
#| ee            Estonia       Tallinn
