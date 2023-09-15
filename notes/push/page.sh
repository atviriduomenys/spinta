# 2023-04-05 09:28

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=push/page
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


test -e $BASEDIR/db.sqlite && rm $BASEDIR/db.sqlite
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
#| _type           _id                                   _revision  _page     id  name      
#| --------------  ------------------------------------  ---------  --------  --  ----------
#| push/page/City  039b67a9-4807-45bf-9b6a-76f958afae17  ∅          WzFd      1   Vilnius
#| push/page/City  05aaa42a-3a4e-45be-8812-b6c8c5188c90  ∅          WzJd      2   Kaunas
#| push/page/City  4d05a2ce-59c8-470b-ae6b-09e60ab870d8  ∅          WzNd      3   Klaipėda
#| push/page/City  3d53c168-0a2c-465a-8753-06c1c5095516  ∅          WzRd      4   Šiauliai
#| push/page/City  64656a30-13ee-44ea-bdd2-14b85687c62e  ∅          WzVd      5   Panevėžys
#| push/page/City  4931c276-93ba-4302-a81d-38b51dc60cdc  ∅          WzZd      6   Alytus
#| push/page/City  3da93658-f746-4507-8940-bdeccb9f33f3  ∅          Wzdd      7   Marijampolė
#| push/page/City  c5999762-8766-4711-b326-6cfff8acc034  ∅          Wzhd      8   Mažeikiai
#| push/page/City  1d8802d4-f52d-4237-b2d9-646ae3aeba7b  ∅          Wzld      9   Jonava
#| push/page/City  6031b3bf-a245-4513-9d4d-55c2a9ae10b6  ∅          WzEwXQ==  10  Utena
                                                                      
echo WzFd | base64 --decode
#| [1]
echo WzJd | base64 --decode
#| [2]

http GET "$SERVER/$DATASET/City?format(ascii)&limit(3)"
#| --------------  ------------------------------------  ---------  -----  -----  --  --------
#| _type           _id                                   _revision  _base  _page  id  name   
#| push/page/City  dcf291ce-835d-4517-85cb-0ef02e2bba0c  ∅          ∅      WzFd   1   Vilnius
#| push/page/City  02b1b42a-f2ae-4587-8d10-07c381944093  ∅          ∅      WzJd   2   Kaunas
#| push/page/City  aae6f1ba-5768-4e26-810c-400186cace57  ∅          ∅      WzNd   3   Klaipėda


http GET "$SERVER/$DATASET/City?format(ascii)&limit(4)"
#| _type           _id                                   _revision  _base  _page  id  name   
#| --------------  ------------------------------------  ---------  -----  -----  --  -------
#| push/page/City  dcf291ce-835d-4517-85cb-0ef02e2bba0c  ∅          ∅      WzFd   1   Vilnius
#| push/page/City  02b1b42a-f2ae-4587-8d10-07c381944093  ∅          ∅      WzJd   2   Kaunas
#| push/page/City  aae6f1ba-5768-4e26-810c-400186cace57  ∅          ∅      WzNd   3   Klaipėda
#| push/page/City  49fc999c-cd97-42ae-a390-b6fea432af50  ∅          ∅      WzRd   4   Šiauliai


http GET "$SERVER/$DATASET/City?format(ascii)&limit(100)"
#| _type           _id                                   _revision  _base  _page     id  name      
#| --------------  ------------------------------------  ---------  -----  --------  --  ----------
#| push/page/City  dcf291ce-835d-4517-85cb-0ef02e2bba0c  ∅          ∅      WzFd      1   Vilnius
#| push/page/City  02b1b42a-f2ae-4587-8d10-07c381944093  ∅          ∅      WzJd      2   Kaunas
#| push/page/City  aae6f1ba-5768-4e26-810c-400186cace57  ∅          ∅      WzNd      3   Klaipėda
#| push/page/City  49fc999c-cd97-42ae-a390-b6fea432af50  ∅          ∅      WzRd      4   Šiauliai
#| push/page/City  331d1f2c-5a43-496e-a503-d6903a432d9f  ∅          ∅      WzVd      5   Panevėžys
#| push/page/City  b0d5f11c-f77d-4e68-82d5-1b371e01916b  ∅          ∅      WzZd      6   Alytus
#| push/page/City  383c5c3d-1767-4fbd-8bcc-7819423a6049  ∅          ∅      Wzdd      7   Marijampolė
#| push/page/City  ac1e4d24-b6ff-4f63-8cc3-727760563941  ∅          ∅      Wzhd      8   Mažeikiai
#| push/page/City  374b862f-d01e-42ac-bcad-0ca8f97aff15  ∅          ∅      Wzld      9   Jonava
#| push/page/City  77327c99-6c06-4476-a0a5-4ef3ff0d1fb7  ∅          ∅      WzEwXQ==  10  Utena


http GET "$SERVER/$DATASET/City?format(ascii)&page('WzVd')"
#| _type           _id                                   _revision  _page     id  name      
#| --------------  ------------------------------------  ---------  --------  --  ----------
#| push/page/City  b0d5f11c-f77d-4e68-82d5-1b371e01916b  ∅          WzZd      6   Alytus
#| push/page/City  383c5c3d-1767-4fbd-8bcc-7819423a6049  ∅          Wzdd      7   Marijampolė
#| push/page/City  ac1e4d24-b6ff-4f63-8cc3-727760563941  ∅          Wzhd      8   Mažeikiai
#| push/page/City  374b862f-d01e-42ac-bcad-0ca8f97aff15  ∅          Wzld      9   Jonava
#| push/page/City  77327c99-6c06-4476-a0a5-4ef3ff0d1fb7  ∅          WzEwXQ==  10  Utena


cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref | source       | prepare             | level | access
$DATASET                  |         |     |              |                     |       |
  | db                    | sql     |     | sqlite:///$BASEDIR/db.sqlite |     |       |
  |   |   | City          |         | id  | cities       | page(name, size: 2) | 4     |
  |   |   |   | id        | integer |     | id           |                     | 4     | open
  |   |   |   | name      | string  |     | name         |                     | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# Run server with external mode (read from db.sqlite directly)
test -n "$PID" && kill $PID
poetry run spinta run --mode external $BASEDIR/manifest.csv &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _page                             id  name      
#| --------------------------------  --  ----------
#| WyJBbHl0dXMiXQ==                  6   Alytus
#| WyJKb25hdmEiXQ==                  9   Jonava
#| WyJLYXVuYXMiXQ==                  2   Kaunas
#| WyJLbGFpcFx1MDExN2RhIl0=          3   Klaipėda
#| WyJNYXJpamFtcG9sXHUwMTE3Il0=      7   Marijampolė
#| WyJNYVx1MDE3ZWVpa2lhaSJd          8   Mažeikiai
#| WyJQYW5ldlx1MDExN1x1MDE3ZXlzIl0=  5   Panevėžys
#| WyJVdGVuYSJd                      10  Utena
#| WyJWaWxuaXVzIl0=                  1   Vilnius
#| WyJcdTAxNjBpYXVsaWFpIl0=          4   Šiauliai

http GET "$SERVER/$DATASET/City?format(ascii)&sort(id)"
#| _page                                 id  name      
#| ------------------------------------  --  ----------
#| WzEsICJWaWxuaXVzIl0=                  1   Vilnius
#| WzIsICJLYXVuYXMiXQ==                  2   Kaunas
#| WzMsICJLbGFpcFx1MDExN2RhIl0=          3   Klaipėda
#| WzQsICJcdTAxNjBpYXVsaWFpIl0=          4   Šiauliai
#| WzUsICJQYW5ldlx1MDExN1x1MDE3ZXlzIl0=  5   Panevėžys
#| WzYsICJBbHl0dXMiXQ==                  6   Alytus
#| WzcsICJNYXJpamFtcG9sXHUwMTE3Il0=      7   Marijampolė
#| WzgsICJNYVx1MDE3ZWVpa2lhaSJd          8   Mažeikiai
#| WzksICJKb25hdmEiXQ==                  9   Jonava
#| WzEwLCAiVXRlbmEiXQ==                  10  Utena

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property  | type    | ref | source       | prepare                 | level | access
$DATASET                  |         |     |              |                         |       |
  | db                    | sql     |     | sqlite:///$BASEDIR/db.sqlite |         |       |
  |   |   | City          |         | id  | cities       | page(id, name, size: 2) | 4     |
  |   |   |   | id        | integer |     | id           |                         | 4     | open
  |   |   |   | name      | string  |     | name         |                         | 4     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/postgres.sh         Reset database

poetry run spinta bootstrap

# notes/spinta/server.sh    Add client
# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Create push state table using an old schema
test -e $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db

# Delete all data from internal database, to start with a clean state.
http DELETE "$SERVER/$DATASET/:wipe" $AUTH

http GET "$SERVER/$DATASET/City?format(ascii)"

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost
#| PUSH: 100%|###| 10/10 [00:00<00:00, 350.74it/s]

sqlite3 $BASEDIR/push/localhost.db 'select * from "_page"'
#| +----------------+----------+-----------------------------+
#| |     model      | property |            value            |
#| +----------------+----------+-----------------------------+
#| | push/page/City | id,name  | {"id": 10, "name": "Utena"} |
#| +----------------+----------+-----------------------------+

sqlite3 $BASEDIR/push/localhost.db 'select * from "push/page/City"'
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+---------+-------------+
#| |                  id                  |                 checksum                 |               revision               |           pushed           | error | data | page.id |  page.name  |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+---------+-------------+
#| | dcf291ce-835d-4517-85cb-0ef02e2bba0c | 866314955098a3fa463e7ad8e224437bb6860094 | d14a502a-a717-4bdb-b1fc-7a5bc8c987ca | 2023-09-15 11:54:47.729962 | 0     |      | 1       | Vilnius     |
#| | 02b1b42a-f2ae-4587-8d10-07c381944093 | 9733ad3c61c891c4888103014102e54755bdef31 | a5660a54-41ab-459b-a88f-f8e2baad1515 | 2023-09-15 11:54:47.731386 | 0     |      | 2       | Kaunas      |
#| | aae6f1ba-5768-4e26-810c-400186cace57 | f47607903f30598ab0805cd6a28fc7c8a0590430 | 2b14455e-ffd7-4059-9861-b4ed0e8e7c56 | 2023-09-15 11:54:47.731735 | 0     |      | 3       | Klaipėda    |
#| | 49fc999c-cd97-42ae-a390-b6fea432af50 | e4a5732061a84cb4f13c16b10672b3537bad0735 | ff67f5f1-b37c-4a4e-a56d-9854e5137dd8 | 2023-09-15 11:54:47.732183 | 0     |      | 4       | Šiauliai    |
#| | 331d1f2c-5a43-496e-a503-d6903a432d9f | 51b4dc274a600b799bbe068e2898fa31e5d9a3ee | 595d0ad4-6fb9-4bd5-87b8-d799e6c47df8 | 2023-09-15 11:54:47.732570 | 0     |      | 5       | Panevėžys   |
#| | b0d5f11c-f77d-4e68-82d5-1b371e01916b | dba0c729d0d33f684d1f77175532943aa5fce91e | 820b236a-0fc1-42f3-8aae-b5181af72e42 | 2023-09-15 11:54:47.733171 | 0     |      | 6       | Alytus      |
#| | 383c5c3d-1767-4fbd-8bcc-7819423a6049 | 73a19c86dffa9363cbaf9f2bd9d1d5fbbe354bc0 | d86cc411-726d-4897-bd7a-b59d9e7d09d3 | 2023-09-15 11:54:47.733456 | 0     |      | 7       | Marijampolė |
#| | ac1e4d24-b6ff-4f63-8cc3-727760563941 | 0989120804b58b747ff998744808f8d3c8188534 | 86b3e49a-d882-4ce9-9487-3cef05135666 | 2023-09-15 11:54:47.733729 | 0     |      | 8       | Mažeikiai   |
#| | 374b862f-d01e-42ac-bcad-0ca8f97aff15 | c64c87682eb80327e3c03ff5f36827abc3151254 | acbc66fc-a26d-41ad-a172-17daf8b4ae25 | 2023-09-15 11:54:47.734023 | 0     |      | 9       | Jonava      |
#| | 77327c99-6c06-4476-a0a5-4ef3ff0d1fb7 | 2e74a59cb754fe07503c51bbaf4c774cea6bf9a0 | eb87e26a-5bd9-48de-af99-e8183f7400b4 | 2023-09-15 11:54:47.734257 | 0     |      | 10      | Utena       |
#| +--------------------------------------+------------------------------------------+--------------------------------------+----------------------------+-------+------+---------+-------------+

http GET "$SERVER/$DATASET/City?format(ascii)"
#| _page                                                                                     id  name      
#| ---------------------------------------------------------------------------------------   --  ----------
#| WzEsICJWaWxuaXVzIiwgImRjZjI5MWNlLTgzNWQtNDUxNy04NWNiLTBlZjAyZTJiYmEwYyJd                  1   Vilnius
#| WzIsICJLYXVuYXMiLCAiMDJiMWI0MmEtZjJhZS00NTg3LThkMTAtMDdjMzgxOTQ0MDkzIl0=                  2   Kaunas
#| WzMsICJLbGFpcFx1MDExN2RhIiwgImFhZTZmMWJhLTU3NjgtNGUyNi04MTBjLTQwMDE4NmNhY2U1NyJd          3   Klaipėda
#| WzQsICJcdTAxNjBpYXVsaWFpIiwgIjQ5ZmM5OTljLWNkOTctNDJhZS1hMzkwLWI2ZmVhNDMyYWY1MCJd          4   Šiauliai
#| WzUsICJQYW5ldlx1MDExN1x1MDE3ZXlzIiwgIjMzMWQxZjJjLTVhNDMtNDk2ZS1hNTAzLWQ2OTAzYTQzMmQ5ZiJd  5   Panevėžys
#| WzYsICJBbHl0dXMiLCAiYjBkNWYxMWMtZjc3ZC00ZTY4LTgyZDUtMWIzNzFlMDE5MTZiIl0=                  6   Alytus
#| WzcsICJNYXJpamFtcG9sXHUwMTE3IiwgIjM4M2M1YzNkLTE3NjctNGZiZC04YmNjLTc4MTk0MjNhNjA0OSJd      7   Marijampolė
#| WzgsICJNYVx1MDE3ZWVpa2lhaSIsICJhYzFlNGQyNC1iNmZmLTRmNjMtOGNjMy03Mjc3NjA1NjM5NDEiXQ==      8   Mažeikiai
#| WzksICJKb25hdmEiLCAiMzc0Yjg2MmYtZDAxZS00MmFjLWJjYWQtMGNhOGY5N2FmZjE1Il0=                  9   Jonava
#| WzEwLCAiVXRlbmEiLCAiNzczMjdjOTktNmMwNi00NDc2LWEwYTUtNGVmM2ZmMGQxZmI3Il0=                  10  Utena


echo "WzEsICJWaWxuaXVzIiwgImRjZjI5MWNlLTgzNWQtNDUxNy04NWNiLTBlZjAyZTJiYmEwYyJd" | base64 --decode
#| [1, "Vilnius", "dcf291ce-835d-4517-85cb-0ef02e2bba0c"]
# XXX: Here I run Spinta with internal mode, whre page key should always be
#      _id, but here I get more than _id. Shouldn't this be change to use only
#      _id?

http GET "$SERVER/$DATASET/City?format(ascii)&page('WzcsICJNYXJpamFtcG9sXHUwMTE3IiwgIjM4M2M1YzNkLTE3NjctNGZiZC04YmNjLTc4MTk0MjNhNjA0OSJd')"
#| _page                                                                                 id  name     
#| ------------------------------------------------------------------------------------  --  ---------
#| WzgsICJNYVx1MDE3ZWVpa2lhaSIsICJhYzFlNGQyNC1iNmZmLTRmNjMtOGNjMy03Mjc3NjA1NjM5NDEiXQ==  8   Mažeikiai
#| WzksICJKb25hdmEiLCAiMzc0Yjg2MmYtZDAxZS00MmFjLWJjYWQtMGNhOGY5N2FmZjE1Il0=              9   Jonava
#| WzEwLCAiVXRlbmEiLCAiNzczMjdjOTktNmMwNi00NDc2LWEwYTUtNGVmM2ZmMGQxZmI3Il0=              10  Utena

http GET "$SERVER/$DATASET/City?id>8&format(ascii)&page('WzcsICJNYXJpamFtcG9sXHUwMTE3IiwgIjM4M2M1YzNkLTE3NjctNGZiZC04YmNjLTc4MTk0MjNhNjA0OSJd')"
#| _page                                                                     id  name  
#| ------------------------------------------------------------------------  --  ------
#| WzksICJKb25hdmEiLCAiMzc0Yjg2MmYtZDAxZS00MmFjLWJjYWQtMGNhOGY5N2FmZjE1Il0=  9   Jonava
#| WzEwLCAiVXRlbmEiLCAiNzczMjdjOTktNmMwNi00NDc2LWEwYTUtNGVmM2ZmMGQxZmI3Il0=  10  Utena

http GET "$SERVER/$DATASET/City?id>8&format(ascii)"
#| _page                                                                     id  name  
#| ------------------------------------------------------------------------  --  ------
#| WzksICJKb25hdmEiLCAiMzc0Yjg2MmYtZDAxZS00MmFjLWJjYWQtMGNhOGY5N2FmZjE1Il0=  9   Jonava
#| WzEwLCAiVXRlbmEiLCAiNzczMjdjOTktNmMwNi00NDc2LWEwYTUtNGVmM2ZmMGQxZmI3Il0=  10  Utena


http GET "$SERVER/$DATASET/City?id>8&format(ascii)&sort(-id)"
#| _page                                                                     id  name 
#| ------------------------------------------------------------------------  --  ------
#| WzEwLCAiVXRlbmEiLCAiNzczMjdjOTktNmMwNi00NDc2LWEwYTUtNGVmM2ZmMGQxZmI3Il0=  10  Utena
#| WzksICJKb25hdmEiLCAiMzc0Yjg2MmYtZDAxZS00MmFjLWJjYWQtMGNhOGY5N2FmZjE1Il0=  9   Jonava

http GET "$SERVER/$DATASET/City?id<8&format(ascii)&sort(name)"
#| _page                                                                                     id  name      
#| ----------------------------------------------------------------------------------------  --  ----------
#| WyJBbHl0dXMiLCA2LCAiYjBkNWYxMWMtZjc3ZC00ZTY4LTgyZDUtMWIzNzFlMDE5MTZiIl0=                  6   Alytus
#| WyJLYXVuYXMiLCAyLCAiMDJiMWI0MmEtZjJhZS00NTg3LThkMTAtMDdjMzgxOTQ0MDkzIl0=                  2   Kaunas
#| WyJLbGFpcFx1MDExN2RhIiwgMywgImFhZTZmMWJhLTU3NjgtNGUyNi04MTBjLTQwMDE4NmNhY2U1NyJd          3   Klaipėda
#| WyJNYXJpamFtcG9sXHUwMTE3IiwgNywgIjM4M2M1YzNkLTE3NjctNGZiZC04YmNjLTc4MTk0MjNhNjA0OSJd      7   Marijampolė
#| WyJQYW5ldlx1MDExN1x1MDE3ZXlzIiwgNSwgIjMzMWQxZjJjLTVhNDMtNDk2ZS1hNTAzLWQ2OTAzYTQzMmQ5ZiJd  5   Panevėžys
#| WyJWaWxuaXVzIiwgMSwgImRjZjI5MWNlLTgzNWQtNDUxNy04NWNiLTBlZjAyZTJiYmEwYyJd                  1   Vilnius
#| WyJcdTAxNjBpYXVsaWFpIiwgNCwgIjQ5ZmM5OTljLWNkOTctNDJhZS1hMzkwLWI2ZmVhNDMyYWY1MCJd          4   Šiauliai


http GET "$SERVER/$DATASET/City?name.startswith('K')&format(ascii)&sort(name)"
#| _page                                                                             id  name   
#| --------------------------------------------------------------------------------  --  --------
#| WyJLYXVuYXMiLCAyLCAiMDJiMWI0MmEtZjJhZS00NTg3LThkMTAtMDdjMzgxOTQ0MDkzIl0=          2   Kaunas
#| WyJLbGFpcFx1MDExN2RhIiwgMywgImFhZTZmMWJhLTU3NjgtNGUyNi04MTBjLTQwMDE4NmNhY2U1NyJd  3   Klaipėda
