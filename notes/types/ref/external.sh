# 2023-03-02 16:43

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/ref/external
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | level | access
$DATASET                 |         |         |       | 
  |   |   | Country      |         | id      |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   | City         |         | id      |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   | country  | ref     | Country | 3     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

poetry run spinta bootstrap

alias query='psql -h localhost -p 54321 -U admin spinta'
query -c '\d "'$DATASET'/City"'
#|                   Table "public.types/ref/external/City"
#|    Column   |            Type             | Collation | Nullable | Default 
#| ------------+-----------------------------+-----------+----------+---------
#|  _txn       | uuid                        |           |          | 
#|  _created   | timestamp without time zone |           |          | 
#|  _updated   | timestamp without time zone |           |          | 
#|  _id        | uuid                        |           | not null | 
#|  _revision  | text                        |           |          | 
#|  id         | integer                     |           |          | 
#|  name       | text                        |           |          | 
#|  country.id | integer                     |           |          | 
#| Indexes:
#|     "types/ref/external/City_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/ref/external/City__txn" btree (_txn)

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/City" $AUTH id:=1 name=Vilnius country:=1
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InvalidRefValue",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "types/ref/external",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/external/City",
#|                 "property": "country",
#|                 "schema": "8",
#|                 "value": 1
#|             },
#|             "message": "Invalid reference value: 1.",
#|             "template": "Invalid reference value: {value}.",
#|             "type": "property"
#|         }
#|     ]
#| }


http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {"_id": 1}
}
EOF
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "types/ref/external",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/external/City",
#|                 "property": "_id",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property '_id'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "property"
#|         }
#|     ]
#| }
# TODO: Show full property name: country._id


http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {"id": 1}
}
EOF
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "a6e02923-234b-4026-8a82-0523cd7a1904",
#|     "_revision": "fded6e81-6b0e-4cad-b70b-2d556abf0f83",
#|     "_type": "types/ref/external/City",
#|     "country": {
#|         "id": 1
#|     },
#|     "id": 1,
#|     "name": "Vilnius"
#| }


http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {"name": "Lithuania"}
}
EOF
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "types/ref/external",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/external/City",
#|                 "property": "name",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property 'name'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "property"
#|         }
#|     ]
#| }
# TODO: Passing other properties should be allowed.
#       https://github.com/atviriduomenys/spinta/issues/262
#       https://github.com/atviriduomenys/spinta/issues/335


http GET "$SERVER/$DATASET/City?select(_id,country)&format(ascii)"
#| _id                                   country.id
#| ------------------------------------  ----------
#| eccfc4f0-94a4-4e11-bae0-c0c5478a6fd9  1


# 2023-03-21 13:12 Test without primary keys


DATASET=$INSTANCE/pkeys

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | level | access
$DATASET                 |         |         |       | 
  |   |   | Country      |         |         |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   | City         |         |         |       | 
  |   |   |   | id       | integer |         | 4     | open
  |   |   |   | name     | string  |         | 4     | open
  |   |   |   | country  | ref     | Country | 3     | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

poetry run spinta bootstrap

alias query='psql -h localhost -p 54321 -U admin spinta'
query -c '\d "'$DATASET'/City"'
#|                 Table "public.types/ref/external/pkeys/City"
#|    Column    |            Type             | Collation | Nullable | Default 
#| -------------+-----------------------------+-----------+----------+---------
#|  _txn        | uuid                        |           |          | 
#|  _created    | timestamp without time zone |           |          | 
#|  _updated    | timestamp without time zone |           |          | 
#|  _id         | uuid                        |           | not null | 
#|  _revision   | text                        |           |          | 
#|  id          | integer                     |           |          | 
#|  name        | text                        |           |          | 
#|  country._id | uuid                        |           |          | 
#| Indexes:
#|     "types/ref/external/pkeys/City_pkey" PRIMARY KEY, btree (_id)
#|     "ix_types/ref/external/pkeys/City__txn" btree (_txn)

test -n "$PID" && kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {"_id": 1}
}
EOF
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "FieldNotInResource",
#|             "context": {
#|                 "attribute": "",
#|                 "component": "spinta.components.Property",
#|                 "dataset": "types/ref/external/pkeys",
#|                 "entity": "",
#|                 "manifest": "default",
#|                 "model": "types/ref/external/pkeys/City",
#|                 "property": "_id",
#|                 "schema": "8"
#|             },
#|             "message": "Unknown property '_id'.",
#|             "template": "Unknown property {property!r}.",
#|             "type": "property"
#|         }
#|     ]
#| }
# TODO: https://github.com/atviriduomenys/spinta/issues/400


http POST "$SERVER/$DATASET/City" $AUTH <<'EOF'
{
    "id": 1,
    "name": "Vilnius",
    "country": {"id": 1}
}
EOF
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "a6e02923-234b-4026-8a82-0523cd7a1904",
#|     "_revision": "fded6e81-6b0e-4cad-b70b-2d556abf0f83",
#|     "_type": "types/ref/external/City",
#|     "country": {
#|         "id": 1
#|     },
#|     "id": 1,
#|     "name": "Vilnius"
#| }


http GET "$SERVER/$DATASET/City?select(_id,country)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "KeyError",
#|             "message": "'country.id'"
#|         }
#|     ]
#| }
# TODO: https://github.com/atviriduomenys/spinta/issues/400
