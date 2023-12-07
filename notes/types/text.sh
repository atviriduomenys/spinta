# 2023-02-17 16:10

git log -1 --oneline
#| 0ebfd06 (HEAD -> geometry-z, origin/master, origin/HEAD, master) Check if public data was written

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/text
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server



cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref | source    | access
$DATASET                 |         |     |           |
  | data                 | sql     | db  |           |
  |   |   | Country      |         | id  | countries |
  |   |   |   | id       | integer |     | id        | open
  |   |   |   | name     | text    |     |           | open
  |   |   |   | name@lt  | string  |     | name_lt   | open
  |   |   |   | name@en  | string  |     | name_en   | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv

ed -vs $BASEDIR/config.yml <<EOF
/backends:/a
  db:
    type: sql
    dsn: sqlite:///$PWD/$BASEDIR/db.sqlite
.
wq
EOF
cat $BASEDIR/config.yml

test -f $BASEDIR/db.sqlite && rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE countries (
    id           INTEGER PRIMARY KEY,
    name_lt      TEXT,
    name_en      TEXT
);
INSERT INTO countries VALUES (1, 'Lietuva', 'Lithuania');
INSERT INTO countries VALUES (2, 'Latvija', 'Latvia');
EOF
sqlite3 $BASEDIR/db.sqlite "SELECT * FROM countries;"
#| +----+---------+-----------+
#| | id | name_lt |  name_en  |
#| +----+---------+-----------+
#| | 1  | Lietuva | Lithuania |
#| | 2  | Latvija | Latvia    |
#| +----+---------+-----------+

# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Country"'
#|                     Table "public.types/text/Country"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  _txn      | uuid                        |           |          | 
#|  _created  | timestamp without time zone |           |          | 
#|  _updated  | timestamp without time zone |           |          | 
#|  _id       | uuid                        |           | not null | 
#|  _revision | text                        |           |          | 
#|  id        | integer                     |           |          | 
#|  name      | jsonb                       |           |          | 
#| Indexes:
#|     "types/text/Country_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/text/Country__txn" btree (_txn)
#|     "types/text/Country_id_key" UNIQUE CONSTRAINT, btree (id)

# notes/spinta/server.sh    Run server

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
test -f $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db

poetry run spinta push $BASEDIR/manifest.csv -o test@localhost

# notes/spinta/client.sh    Configure client

http GET "$SERVER/$DATASET/Country"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "e4a346e9-5848-4799-af2a-18b4e2acb146",
#|             "_page": "WzEsICJlNGEzNDZlOS01ODQ4LTQ3OTktYWYyYS0xOGI0ZTJhY2IxNDYiXQ==",
#|             "_revision": "33e41832-115d-4f07-a514-e5cc02708370",
#|             "_type": "types/text/Country",
#|             "id": 1,
#|             "name": "Lietuva"
#|         },
#|         {
#|             "_id": "f077eee9-d827-4451-bc0a-7ff1da18a36e",
#|             "_page": "WzIsICJmMDc3ZWVlOS1kODI3LTQ0NTEtYmMwYS03ZmYxZGExOGEzNmUiXQ==",
#|             "_revision": "2791ea9c-1c80-4854-a017-c8aed9e82301",
#|             "_type": "types/text/Country",
#|             "id": 2,
#|             "name": "Latvija"
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country" Accept-Language:en
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "e4a346e9-5848-4799-af2a-18b4e2acb146",
#|             "_page": "WzEsICJlNGEzNDZlOS01ODQ4LTQ3OTktYWYyYS0xOGI0ZTJhY2IxNDYiXQ==",
#|             "_revision": "33e41832-115d-4f07-a514-e5cc02708370",
#|             "_type": "types/text/Country",
#|             "id": 1,
#|             "name": "Lithuania"
#|         },
#|         {
#|             "_id": "f077eee9-d827-4451-bc0a-7ff1da18a36e",
#|             "_page": "WzIsICJmMDc3ZWVlOS1kODI3LTQ0NTEtYmMwYS03ZmYxZGExOGEzNmUiXQ==",
#|             "_revision": "2791ea9c-1c80-4854-a017-c8aed9e82301",
#|             "_type": "types/text/Country",
#|             "id": 2,
#|             "name": "Latvia"
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Country/e4a346e9-5848-4799-af2a-18b4e2acb146"
#| {
#|     "_id": "e4a346e9-5848-4799-af2a-18b4e2acb146",
#|     "_revision": "33e41832-115d-4f07-a514-e5cc02708370",
#|     "_type": "types/text/Country",
#|     "id": 1,
#|     "name": {
#|         "en": "Lithuania",
#|         "lt": "Lietuva"
#|     }
#| }


http GET "$SERVER/$DATASET/Country?select(name)&format(ascii)"
#| name   
#| -------
#| Lietuva
#| Latvija

http GET "$SERVER/$DATASET/Country?select(name@lt)&format(ascii)"
#| name.lt
#| -------
#| Lietuva
#| Latvija

http GET "$SERVER/$DATASET/Country?select(name@lt, name@en)&format(ascii)"
#| name.lt  name.en  
#| -------  ---------
#| Lietuva  Lithuania
#| Latvija  Latvia

http GET "$SERVER/$DATASET/Country?name='Lietuva'&select(name)&format(ascii)" Accept-Language:lt
#| name   
#| -------
#| Lietuva

http GET "$SERVER/$DATASET/Country?name@en='Lithuania'&select(name)&format(ascii)" Accept-Language:lt
#| name   
#| -------
#| Lietuva

http GET "$SERVER/$DATASET/Country?select(name)&sort(name)&format(ascii)" Accept-Language:lt
#| name   
#| -------
#| Latvija
#| Lietuva

http GET "$SERVER/$DATASET/Country?select(name)&sort(-name@en)&format(ascii)" Accept-Language:lt
#| name   
#| -------
#| Lietuva
#| Latvija

http GET "$SERVER/$DATASET/Country?format(rdf)"
#| <?xml version="1.0" encoding="UTF-8"?>
#| <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:pav="http://purl.org/pav/" xmlns="http://localhost:8000/">
#|   <rdf:Description rdf:about="/types/text/Country/e4a346e9-5848-4799-af2a-18b4e2acb146" rdf:type="types/text/Country" pav:version="33e41832-115d-4f07-a514-e5cc02708370">
#|     <_page>WzEsICJlNGEzNDZlOS01ODQ4LTQ3OTktYWYyYS0xOGI0ZTJhY2IxNDYiXQ==</_page>
#|     <id>1</id>
#|     <name>Lietuva</name>
# TODO: Should be
#         <name xml:lang="lt">Lietuva</name>
#       https://github.com/atviriduomenys/spinta/issues/549
#|   </rdf:Description>
#|   <rdf:Description rdf:about="/types/text/Country/f077eee9-d827-4451-bc0a-7ff1da18a36e" rdf:type="types/text/Country" pav:version="2791ea9c-1c80-4854-a017-c8aed9e82301">
#|     <_page>WzIsICJmMDc3ZWVlOS1kODI3LTQ0NTEtYmMwYS03ZmYxZGExOGEzNmUiXQ==</_page>
#|     <id>2</id>
#|     <name>Latvija</name>
#|   </rdf:Description>
#| </rdf:RDF>

xdg-open http://localhost:8000/$DATASET/Country
xdg-open http://localhost:8000/$DATASET/Country/:changes/-10

http GET "$SERVER/$DATASET/Country/:changes/-10"
#| {
#|     "_data": [
#|         {
#|             "_cid": 1,
#|             "_created": "2023-11-21T07:14:59.642472",
#|             "_id": "e4a346e9-5848-4799-af2a-18b4e2acb146",
#|             "_op": "insert",
#|             "_revision": "33e41832-115d-4f07-a514-e5cc02708370",
#|             "_txn": "0770f48f-be39-4593-9341-deda7cef0a85",
#|             "id": 1,
#|             "name": {
#|                 "en": "Lithuania",
#|                 "lt": "Lietuva"
#|             }
#|         },
#|         {
#|             "_cid": 2,
#|             "_created": "2023-11-21T07:14:59.642472",
#|             "_id": "f077eee9-d827-4451-bc0a-7ff1da18a36e",
#|             "_op": "insert",
#|             "_revision": "2791ea9c-1c80-4854-a017-c8aed9e82301",
#|             "_txn": "0770f48f-be39-4593-9341-deda7cef0a85",
#|             "id": 2,
#|             "name": {
#|                 "en": "Latvia",
#|                 "lt": "Latvija"
#|             }
#|         }
#|     ]
#| }
http GET "$SERVER/$DATASET/Country/:changes/-10?format(ascii)"
#| _cid  _created                    _op     _id                                   _txn                                  _revision                             id  name
#| ----  --------------------------  ------  ------------------------------------  ------------------------------------  ------------------------------------  --  ----
#| 1     2023-11-21T07:14:59.642472  insert  e4a346e9-5848-4799-af2a-18b4e2acb146  0770f48f-be39-4593-9341-deda7cef0a85  33e41832-115d-4f07-a514-e5cc02708370  1   ∅
#| 2     2023-11-21T07:14:59.642472  insert  f077eee9-d827-4451-bc0a-7ff1da18a36e  0770f48f-be39-4593-9341-deda7cef0a85  2791ea9c-1c80-4854-a017-c8aed9e82301  2   ∅
# TODO: `name` should be expanded into `name.en` and `name.lt`.
#       https://github.com/atviriduomenys/spinta/issues/550
