# 2023-04-25 07:55

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=backends/postgresql/pkeys/unique
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref      | access
$DATASET                    |         |          |
  |   |   | No              |         |          |
  |   |   |   | id          | integer |          | open
  |   |   |   | code        | string  |          | open
  |   |   |   | title       | string  |          | open
  |   |   | Single          |         | id       |
  |   |   |   | id          | integer |          | open
  |   |   |   | code        | string  |          | open
  |   |   |   | title       | string  |          | open
  |   |   | Multiple        |         | id, code |
  |   |   |   | id          | integer |          | open
  |   |   |   | code        | string  |          | open
  |   |   |   | title       | string  |          | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/No"'
#| PRIMARY KEY, btree (_id)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Single"'
#| PRIMARY KEY, btree (_id)
#| UNIQUE CONSTRAINT, btree (id)
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Multiple"'
#| PRIMARY KEY, btree (_id)
#| UNIQUE CONSTRAINT, btree (id, code)

# No primary key set

http POST "$SERVER/$DATASET/No"       $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "fc3b4710-f70a-4dfd-8b31-d730f43aacfd",
#|     "_revision": "248aa3db-8a20-474c-a491-0c65a0b6f087",
#|     "_type": "backends/postgresql/pkeys/unique/No",
#|     "code": "01",
#|     "id": 1,
#|     "title": "T01"
#| }

http POST "$SERVER/$DATASET/No"       $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "6ac6aa3b-b3cb-4e14-a6ba-b31afe47702e",
#|     "_revision": "0fdd0dc4-82ca-4793-92e4-abf82fe5b73c",
#|     "_type": "backends/postgresql/pkeys/unique/No",
#|     "code": "01",
#|     "id": 1,
#|     "title": "T01"
#| }


# Single property primary key

http POST "$SERVER/$DATASET/Single"   $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "467af398-60ee-412a-b1c6-2019fc2b8f4e",
#|     "_revision": "27682f2c-1a45-4bd1-beb2-a992b1770f44",
#|     "_type": "backends/postgresql/pkeys/unique/Single",
#|     "code": "01",
#|     "id": 1,
#|     "title": "T01"
#| }

http POST "$SERVER/$DATASET/Single"   $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "UniqueConstraint",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "backends/postgresql/pkeys/unique",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "backends/postgresql/pkeys/unique/Single",
#|                 "property": "id",
#|                 "schema": "9"
#|             },
#|             "message": "Given value already exists.",
#|             "template": "Given value already exists.",
#|             "type": "property"
#|         }
#|     ]
#| }

http POST "$SERVER/$DATASET/Single"   $AUTH id:=2 code=02 title=T02
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "98918e9a-8760-4c21-9fe2-6e59eba5d2d9",
#|     "_revision": "1e5defe0-f086-4347-9d4e-74ad3aab67f4",
#|     "_type": "backends/postgresql/pkeys/unique/Single",
#|     "code": "02",
#|     "id": 2,
#|     "title": "T02"
#| }


# Multi property primary key (composite key)

http POST "$SERVER/$DATASET/Multiple" $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "30402108-60e1-41a0-80c7-7dce57c96d05",
#|     "_revision": "0112c12b-2353-411b-9d2e-5d4e738d3a36",
#|     "_type": "backends/postgresql/pkeys/unique/Multiple",
#|     "code": "01",
#|     "id": 1,
#|     "title": "T01"
#| }

http POST "$SERVER/$DATASET/Multiple" $AUTH id:=1 code=01 title=T01
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "CompositeUniqueConstraint",
#|             "context": {
#|                 "component": "spinta.components.Model",
#|                 "dataset": "backends/postgresql/pkeys/unique",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "backends/postgresql/pkeys/unique/Multiple",
#|                 "properties": "id,code",
#|                 "schema": "14"
#|             },
#|             "message": "Given values for composition of properties (id,code) already exist.",
#|             "template": "Given values for composition of properties ({properties}) already exist.",
#|             "type": "model"
#|         }
#|     ]
#| }

http POST "$SERVER/$DATASET/Multiple" $AUTH id:=1 code=02 title=T02
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "8e176912-ba1d-4e37-b496-900ad5f97bed",
#|     "_revision": "9e0e357f-296c-41db-8da0-8eba634fa90f",
#|     "_type": "backends/postgresql/pkeys/unique/Multiple",
#|     "code": "02",
#|     "id": 1,
#|     "title": "T02"
#| }
