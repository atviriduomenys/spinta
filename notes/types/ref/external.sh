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

restart-spinta () {
    test -n "$PID" && kill $PID
    poetry run spinta run &>> $BASEDIR/spinta.log &; PID=$!
}

restart-spinta

http POST "$SERVER/$DATASET/City" $AUTH id:=1 name=Vilnius country:=1
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InvalidNestedValue",
#|             "context": {
#|                 "value": 1
#|             },
#|             "message": "Expected value to be dictionary, got 1.",
#|             "template": "Expected value to be dictionary, got {value}.",
#|             "type": "system"
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
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "05f29bc1-5c54-463c-ad99-2cddb58cd62c",
#|     "_revision": "0dda7963-40e6-4fae-92cd-24b624796a1b",
#|     "_type": "types/ref/external/City",
#|     "country": {
#|         "_id": 1
#|     },
#|     "id": 1,
#|     "name": "Vilnius"
#| }
# FIXME: We should not allow to pass _id for ExternalRef

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
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "bcaadf76-62f9-4a96-8d1f-2acd7f3f84f3",
#|     "_revision": "774c726f-4d21-476e-84df-55cc8d2fbda6",
#|     "_type": "types/ref/external/City",
#|     "country": {
#|         "name": "Lithuania"
#|     },
#|     "id": 1,
#|     "name": "Vilnius"
#| }
# FIXME: We should not allow to pass any other fields, unless passed field is
#        denormalized.


http GET "$SERVER/$DATASET/City?select(_id,country)&format(ascii)"
#| ------------------------------------  ----------
#| _id                                   country.id
#| 05f29bc1-5c54-463c-ad99-2cddb58cd62c  ∅
#| a6e02923-234b-4026-8a82-0523cd7a1904  1
#| bcaadf76-62f9-4a96-8d1f-2acd7f3f84f3  ∅
#| d4f0aadb-8d9e-4b3f-856e-8d711935417a  ∅
#| ------------------------------------  ----------
