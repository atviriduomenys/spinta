# 2023-03-01 10:12

git log -1 --oneline
#| ea345f2 (HEAD -> 259-required-not-null-constraint, origin/259-required-not-null-constraint) 259 fix PropertyReader

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=metadata/null
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                     | access
$DATASET                 |                          | 
  |   |   | City         |                          | 
  |   |   |   | name     | string required          | open
  |   |   |   | country  | string                   | open
  |   |   |   | koords   | geometry(point) required | open
EOF

poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show
poetry run spinta check

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/City"'
#|                     Table "public.metadata/null/City"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  name      | text                        |           | not null | 
#|  country   | text                        |           |          | 
#|  koords    | geometry(Point)             |           |          | 
# FIXME: koords should be not null

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/City" $AUTH \
    name="Vilnius" \
    country="lt" \
    koords="POINT (54.687222 25.28)"
#| HTTP/1.1 201 Created

http POST "$SERVER/$DATASET/City" $AUTH country="lt"
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "RequiredProperty",
#|             "context": {
#|                 "model": "metadata/null/City",
#|                 "prop": "name"
#|             },
#|             "message": "name is required for metadata/null/City.",
#|             "template": "{prop} is required for {model}.",
#|             "type": "system"
#|         }
#|     ]
#| }
# FIXME: type should be property
# TODO: multiple errors should be shown, in this case koords is also a required
#       property, but is not shown in the message
#       https://github.com/atviriduomenys/spinta/issues/378


http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "d176699e-c589-405c-a4e8-3cdd2c24e54b",
#|             "_revision": "89bb5e9b-7613-433d-b6f2-6003a9cfe59c",
#|             "_type": "metadata/null/City",
#|             "country": "lt",
#|             "name": "Vilnius"
#|         }
#|     ]
#| }


http PATCH "$SERVER/$DATASET/City/d176699e-c589-405c-a4e8-3cdd2c24e54b" $AUTH \
    _revision=89bb5e9b-7613-433d-b6f2-6003a9cfe59c \
    name:=null
#| HTTP/1.1 400 Bad Request
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "RequiredProperty",
#|             "context": {
#|                 "model": "metadata/null/City",
#|                 "prop": "name"
#|             },
#|             "message": "name is required for metadata/null/City.",
#|             "template": "{prop} is required for {model}.",
#|             "type": "system"
#|         }
#|     ]
#| }

http PATCH "$SERVER/$DATASET/City/d176699e-c589-405c-a4e8-3cdd2c24e54b" $AUTH \
    _revision=89bb5e9b-7613-433d-b6f2-6003a9cfe59c \
    name=Kaunas
#| HTTP/1.1 200 OK


http GET "$SERVER/$DATASET/City"
#| {
#|     "_data": [
#|         {
#|             "_id": "d176699e-c589-405c-a4e8-3cdd2c24e54b",
#|             "_revision": "99381d44-9e72-4f7a-a61c-ba68cc48205c",
#|             "_type": "metadata/null/City",
#|             "country": "lt",
#|             "name": "Kaunas"
#|         }
#|     ]
#| }
