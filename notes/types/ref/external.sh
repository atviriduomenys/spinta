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

restart-spinta

http POST "$SERVER/$DATASET/City" $AUTH id:=1 name=Vilnius country:=1
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "TypeError",
#|             "message": "argument of type 'int' is not iterable"
#|         }
#|     ]
#| }
tail -100 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 201, in create_http_response
#|     return await commands.push(
#|   File "spinta/commands/write.py", line 100, in push
#|     status_code, response = await simple_response(
#|   File "spinta/commands/write.py", line 1136, in simple_response
#|     results = await alist(aslice(results, 2))
#|   File "spinta/utils/aiotools.py", line 51, in alist
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 51, in <listcomp>
#|     return [x async for x in it]
#|   File "spinta/utils/aiotools.py", line 62, in aslice
#|     async for x in it:
#|   File "spinta/commands/write.py", line 185, in push_stream
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/changes.py", line 25, in create_changelog_entry
#|     async for data in dstream:
#|   File "spinta/backends/postgresql/commands/write.py", line 24, in insert
#|     patch = commands.before_write(context, model, backend, data=data)
#|   File "spinta/backends/postgresql/commands/write.py", line 115, in before_write
#|     value = commands.before_write(
#|   File "spinta/commands/write.py", line 1083, in before_write
#|     patch = take([prop.place for prop in dtype.refprops], data.patch)
#|   File "spinta/utils/data.py", line 74, in take
#|     if v and x in v:
#| TypeError: argument of type 'int' is not iterable
# FIXME: Not related to this task, but could you fix this and return a propper
#        400 error message?

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
