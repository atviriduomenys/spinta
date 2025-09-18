# 2023-02-24 19:31

git log -1 --oneline
#| acb9f1d (HEAD -> wipe-test-notes, origin/master, origin/HEAD, origin/359-rewrite-ascii-table-formatter, master) Merge pull request #333 from atviriduomenys/308-add-export-to-rdf-support

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=wipe
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type    | ref     | access
$DATASET/ds1             |         |         |
  |   |   | City         |         | id      |
  |   |   |   | id       | integer |         | open
  |   |   |   | name     | string  |         | open
$DATASET/ds2             |         |         |
  |   |   | City         |         | id      |
  |   |   |   | id       | integer |         | open
  |   |   |   | name     | string  |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client
# notes/spinta/push.sh      Configure client

http POST "$SERVER/$DATASET/ds1/City" $AUTH id:=1 name=Vilnius
http POST "$SERVER/$DATASET/ds2/City" $AUTH id:=1 name=Vilnius

http GET "$SERVER/$DATASET/ds1/City?format(ascii)"
http GET "$SERVER/$DATASET/ds2/City?format(ascii)"

http DELETE "$SERVER/$DATASET/ds1/:ns/:wipe" $AUTH
#| HTTP/1.1 200 OK
#| 
#| {
#|     "wiped": true
#| }

http GET "$SERVER/$DATASET/ds1/City?format(ascii)"
# No data.
http GET "$SERVER/$DATASET/ds2/City?format(ascii)"
#| -------------  ------------------------------------  ------------------------------------  --  -------
#| _type          _id                                   _revision                             id  name
#| wipe/ds2/City  50496a3b-8887-49d6-82a2-08cb2617a4cd  16d93078-1ef9-4e33-baaa-ac5c437d4ec9  1   Vilnius
#| -------------  ------------------------------------  ------------------------------------  --  -------

http POST "$SERVER/$DATASET/ds1/City" $AUTH id:=1 name=Vilnius
http POST "$SERVER/$DATASET/ds1/City" $AUTH id:=2 name=Kaunas
http GET  "$SERVER/$DATASET/ds1/City?format(ascii)"
#| -------------  ------------------------------------  ------------------------------------  --  -------
#| _type          _id                                   _revision                             id  name
#| wipe/ds1/City  67f80629-7c71-4683-8319-a5c93aae1eab  2f51f3db-7e6e-4c9e-a333-b1effe85a8ab  1   Vilnius
#| wipe/ds1/City  0816cc0c-b3ff-4a5b-87a2-f118e49312df  d4f18c94-5663-46c9-b584-53a27bd12de7  2   Kaunas
#| -------------  ------------------------------------  ------------------------------------  --  -------

http GET  "$SERVER/$DATASET/ds1/City/:changes?select(_cid,_op,id,name)&format(ascii)"
#| ----  ------  --  -------
#| _cid  _op     id  name
#| 1     insert  1   Vilnius
#| 2     insert  2   Kaunas
#| ----  ------  --  -------
# _cid was reset as it should be

http GET  "$SERVER/$DATASET/ds2/City?format(ascii)"
#| -------------  ------------------------------------  ------------------------------------  --  -------
#| _type          _id                                   _revision                             id  name
#| wipe/ds2/City  50496a3b-8887-49d6-82a2-08cb2617a4cd  16d93078-1ef9-4e33-baaa-ac5c437d4ec9  1   Vilnius
#| -------------  ------------------------------------  ------------------------------------  --  -------

http DELETE "$SERVER/$DATASET/ds2/City/:wipe" $AUTH
#| HTTP/1.1 200 OK
#| 
#| {
#|     "wiped": true
#| }

http GET  "$SERVER/$DATASET/ds1/City?format(ascii)"
#| -------------  ------------------------------------  ------------------------------------  --  -------
#| _type          _id                                   _revision                             id  name
#| wipe/ds1/City  67f80629-7c71-4683-8319-a5c93aae1eab  2f51f3db-7e6e-4c9e-a333-b1effe85a8ab  1   Vilnius
#| wipe/ds1/City  0816cc0c-b3ff-4a5b-87a2-f118e49312df  d4f18c94-5663-46c9-b584-53a27bd12de7  2   Kaunas
#| -------------  ------------------------------------  ------------------------------------  --  -------

http GET  "$SERVER/$DATASET/ds2/City?format(ascii)"
# No data.

http POST "$SERVER/$DATASET/ds2/City" $AUTH id:=3 name=Šiauliai
http POST "$SERVER/$DATASET/ds1/City" $AUTH id:=4 name=Panevėžys

http GET  "$SERVER/$DATASET/:all"
#| {
#|     "_data": [
#|         {
#|             "_id": "67f80629-7c71-4683-8319-a5c93aae1eab",
#|             "_revision": "2f51f3db-7e6e-4c9e-a333-b1effe85a8ab",
#|             "_type": "wipe/ds1/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         },
#|         {
#|             "_id": "0816cc0c-b3ff-4a5b-87a2-f118e49312df",
#|             "_revision": "d4f18c94-5663-46c9-b584-53a27bd12de7",
#|             "_type": "wipe/ds1/City",
#|             "id": 2,
#|             "name": "Kaunas"
#|         },
#|         {
#|             "_id": "961ba630-eb8a-4126-a9e7-3219987f9abc",
#|             "_revision": "87d72072-a3da-481d-9ed7-3c080be525a3",
#|             "_type": "wipe/ds1/City",
#|             "id": 4,
#|             "name": "Panevėžys"
#|         },
#|         {
#|             "_id": "cc5a1392-4bc0-4bbf-8d3a-52aed549396e",
#|             "_revision": "736800d0-e5fe-463a-89f6-a91d3b73f8b7",
#|             "_type": "wipe/ds2/City",
#|             "id": 3,
#|             "name": "Šiauliai"
#|         }
#|     ]
#| }

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
#| HTTP/1.1 200 OK
#| 
#| {
#|     "wiped": true
#| }

http GET  "$SERVER/$DATASET/:all"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": []
#| }

http POST "$SERVER/$DATASET/ds1/City" $AUTH id:=1 name=Vilnius
http POST "$SERVER/$DATASET/ds2/City" $AUTH id:=1 name=Vilnius

CLIENT=wipe
cat ~/.config/spinta/clients/$CLIENT.yml
poetry run spinta client add -n $CLIENT -s secret --add-secret --scope - <<EOF
uapi:/:getone
uapi:/:getall
uapi:/:search
uapi:/:changes
uapi:/:create
uapi:/:update
uapi:/:patch
uapi:/:delete
uapi:/${DATASET}/ds1/:wipe
EOF

kill $PID
poetry run spinta run &>> $BASEDIR/spinta.log &
PID=$!
tail -50 $BASEDIR/spinta.log

SCOPES=(
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:changes
  uapi:/:create
  uapi:/:update
  uapi:/:patch
  uapi:/:delete
  uapi:/${DATASET}/ds1/:wipe
)
TOKEN=$(
    http \
        -a $CLIENT:$SECRET \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="$SCOPES" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH

http GET  "$SERVER/$DATASET/:all"
#| {
#|     "_data": [
#|         {
#|             "_id": "39ba7c7e-0170-4c1d-be1a-96534c8cead4",
#|             "_revision": "d94c3eda-49d2-4f94-b23c-c9c07ba7f818",
#|             "_type": "wipe/ds1/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         },
#|         {
#|             "_id": "e09bf1bf-334b-459b-b41e-aa8cc35a678c",
#|             "_revision": "0718ee4a-a84f-401c-9d0d-cb144da0e407",
#|             "_type": "wipe/ds2/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         }
#|     ]
#| }

http DELETE "$SERVER/:wipe" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: uapi:/:wipe"
#|         }
#|     ]
#| }

http DELETE "$SERVER/$DATASET/:wipe" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: uapi:/:wipe, uapi:/wipe/:wipe"
#|         }
#|     ]
#| }

http DELETE "$SERVER/$DATASET/ds2/:wipe" $AUTH
#| HTTP/1.1 403 Forbidden
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "InsufficientScopeError",
#|             "message": "insufficient_scope: Missing one of scopes: uapi:/:wipe, uapi:/wipe/ds2/:wipe, uapi:/wipe/:wipe"
#|         }
#|     ]
#| }

http GET  "$SERVER/:all"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "39ba7c7e-0170-4c1d-be1a-96534c8cead4",
#|             "_revision": "d94c3eda-49d2-4f94-b23c-c9c07ba7f818",
#|             "_type": "wipe/ds1/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         },
#|         {
#|             "_id": "e09bf1bf-334b-459b-b41e-aa8cc35a678c",
#|             "_revision": "0718ee4a-a84f-401c-9d0d-cb144da0e407",
#|             "_type": "wipe/ds2/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         }
#|     ]
#| }

http DELETE "$SERVER/$DATASET/ds1/:wipe" $AUTH
#| HTTP/1.1 200 OK
#| 
#| {
#|     "wiped": true
#| }

http GET  "$SERVER/:all"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "e09bf1bf-334b-459b-b41e-aa8cc35a678c",
#|             "_revision": "0718ee4a-a84f-401c-9d0d-cb144da0e407",
#|             "_type": "wipe/ds2/City",
#|             "id": 1,
#|             "name": "Vilnius"
#|         }
#|     ]
#| }
