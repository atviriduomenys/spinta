# 2023-04-05 09:28

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/page
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS cities (
    id   INTEGER PRIMARY KEY,
    name TEXT
);
INSERT INTO cities VALUES
    (1, 'Vilnius'),
    (2, 'Kaunas'),
    (3, 'Klaipėda'),
    (4, 'Šiauliai'),
    (5, 'Panevėžys'),
    (6, 'Alytus'),
    (7, 'Marijampolė'),
    (8, 'Mažeikiai'),
    (9, 'Jonava'),
    (10, 'Utena');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM cities;"
#| +----+-------------+
#| | id |    name     |
#| +----+-------------+
#| | 1  | Vilnius     |
#| | 2  | Kaunas      |
#| | 3  | Klaipėda    |
#| | 4  | Šiauliai    |
#| | 5  | Panevėžys   |
#| | 6  | Alytus      |
#| | 7  | Marijampolė |
#| | 8  | Mažeikiai   |
#| | 9  | Jonava      |
#| | 10 | Utena       |
#| +----+-------------+

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref | source       | prepare           | level | access
$DATASET                  |         |     |              |                   |       |
  | db                    | sql     |     | sqlite:///$BASEDIR/db.sqlite |   |       |
  |   |   | City          |         | id  | cities       | page(id, size: 2) | 4     |
  |   |   |   | id        | integer |     | id           |                   | 4     | open
  |   |   |   | name      | string  |     | name         |                   | 4     | open
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
#| _type           _id                                   _revision  id  name      
#| --------------  ------------------------------------  ---------  --  ----------
#| push/page/City  039b67a9-4807-45bf-9b6a-76f958afae17  ∅          1   Vilnius
#| push/page/City  05aaa42a-3a4e-45be-8812-b6c8c5188c90  ∅          2   Kaunas
#| push/page/City  4d05a2ce-59c8-470b-ae6b-09e60ab870d8  ∅          3   Klaipėda
#| push/page/City  3d53c168-0a2c-465a-8753-06c1c5095516  ∅          4   Šiauliai
#| push/page/City  64656a30-13ee-44ea-bdd2-14b85687c62e  ∅          5   Panevėžys
#| push/page/City  4931c276-93ba-4302-a81d-38b51dc60cdc  ∅          6   Alytus
#| push/page/City  3da93658-f746-4507-8940-bdeccb9f33f3  ∅          7   Marijampolė
#| push/page/City  c5999762-8766-4711-b326-6cfff8acc034  ∅          8   Mažeikiai
#| push/page/City  1d8802d4-f52d-4237-b2d9-646ae3aeba7b  ∅          9   Jonava
#| push/page/City  6031b3bf-a245-4513-9d4d-55c2a9ae10b6  ∅          10  Utena

# notes/postgres.sh         Reset database

poetry run spinta bootstrap

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "types/ref/external/City"'

# notes/spinta/server.sh    Add client
# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Create push state table using an old schema
rm $BASEDIR/push/localhost.db

# Delete all data from internal database, to start with a clean state.
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|###| 10/10 [00:00<00:00, 232.44it/s]

# Push state database tables should be updates to a new schema
sqlite3 $BASEDIR/push/localhost.db '.schema'
#| CREATE TABLE _page (
#|         model TEXT NOT NULL, 
#|         property TEXT, 
#|         value TEXT, 
#|         PRIMARY KEY (model)
#| );
#| CREATE TABLE IF NOT EXISTS "push/page/City" (
#|         id VARCHAR NOT NULL, 
#|         checksum VARCHAR, 
#|         revision VARCHAR, 
#|         pushed DATETIME, 
#|         error BOOLEAN, 
#|         data TEXT, 
#|         PRIMARY KEY (id)
#| );

sqlite3 $BASEDIR/push/localhost.db 'select * from "_page"'
#| +----------------+----------+-------+
#| |     model      | property | value |
#| +----------------+----------+-------+
#| | push/page/City | id       | 10    |
#| +----------------+----------+-------+

sqlite3 $BASEDIR/push/localhost.db 'select * from "push/page/City"'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | error | data |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+
#| | 039b67a9-4807-45bf-9b6a-76f958afae17 | 866314955098a3fa463e7ad8e224437bb6860094 | ab305ace-777c-4119-a145-885ef93798ef | 2023-04-05 16:58:32.921821 | 0     |      |
#| | 05aaa42a-3a4e-45be-8812-b6c8c5188c90 | 9733ad3c61c891c4888103014102e54755bdef31 | 8d906af0-df71-4653-a667-cc87712f51b3 | 2023-04-05 16:58:32.923231 | 0     |      |
#| | 4d05a2ce-59c8-470b-ae6b-09e60ab870d8 | f47607903f30598ab0805cd6a28fc7c8a0590430 | 3fbf1804-2d2c-4a04-904d-39c8ddd532f4 | 2023-04-05 16:58:32.930369 | 0     |      |
#| | 3d53c168-0a2c-465a-8753-06c1c5095516 | e4a5732061a84cb4f13c16b10672b3537bad0735 | e1ba71ad-992f-4e56-8407-56110f4b7069 | 2023-04-05 16:58:32.930803 | 0     |      |
#| | 64656a30-13ee-44ea-bdd2-14b85687c62e | 51b4dc274a600b799bbe068e2898fa31e5d9a3ee | 87994b96-de56-4390-b91e-e33c1d987f3a | 2023-04-05 16:58:32.937434 | 0     |      |
#| | 4931c276-93ba-4302-a81d-38b51dc60cdc | dba0c729d0d33f684d1f77175532943aa5fce91e | 0cabae3b-aba6-41df-ae85-5540238a8381 | 2023-04-05 16:58:32.937891 | 0     |      |
#| | 3da93658-f746-4507-8940-bdeccb9f33f3 | 73a19c86dffa9363cbaf9f2bd9d1d5fbbe354bc0 | 85a2f8af-ff24-4e5b-aa31-6c40ef9db60e | 2023-04-05 16:58:32.943983 | 0     |      |
#| | c5999762-8766-4711-b326-6cfff8acc034 | 0989120804b58b747ff998744808f8d3c8188534 | 107daf59-1e93-499c-b860-e4a2ab77eb68 | 2023-04-05 16:58:32.944390 | 0     |      |
#| | 1d8802d4-f52d-4237-b2d9-646ae3aeba7b | c64c87682eb80327e3c03ff5f36827abc3151254 | 368dc3eb-efa7-43f4-8974-52ff0f761d96 | 2023-04-05 16:58:32.950502 | 0     |      |
#| | 6031b3bf-a245-4513-9d4d-55c2a9ae10b6 | 2e74a59cb754fe07503c51bbaf4c774cea6bf9a0 | b4610d4a-24e8-4d84-8be0-67c2f0313b0f | 2023-04-05 16:58:32.950889 | 0     |      |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+


