# notes/sqlite.sh           Configure SQLite

INSTANCE=ufuncs/swap
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
INSERT INTO cities VALUES (1, 'Vilnius');
INSERT INTO cities VALUES (2, 'Vėlnius');
INSERT INTO cities VALUES (3, 'Vilna');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"

cat > $BASEDIR/manifest.txt <<EOF
d | r |m | property | type    | ref | source                        | prepare         | level | access
$DATASET            |         |     |                               |                 |       |
  | db              | sql     |     | sqlite:///$BASEDIR/db.sqlite  |                 |       |
  |   |City         |         | id  | cities                        |                 | 4     |
  |   |  | id       | integer |     | id                            |                 | 4     | open
  |   |  | name     | string  |     | name                          |                 | 4     | open
  |   |  |          |         |     | Vėlnius                       | swap("Vilnius") |       |     
  |   |  |          |         |     | Vilna                         | swap("Vilnius") |       |     
  |   |  |          |         |     | Kauna                         | swap("Kaunas")  |       |     
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _type             _id                                   _revision  id  name   
#| ----------------  ------------------------------------  ---------  --  -------
#| ufuncs/swap/City  c81430b7-d214-414a-a3a1-f5f6087b09e4  ∅          1   Vilnius
#| ufuncs/swap/City  d8ac4016-a8fd-493f-9bf4-63150c8ce9fa  ∅          2   Vilnius
#| ufuncs/swap/City  8c9a6563-e203-4806-ae18-ed69de3e5831  ∅          3   Vilnius
