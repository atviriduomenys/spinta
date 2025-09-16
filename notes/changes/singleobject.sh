# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=changes/singleobject
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property    | type    | ref     | access
$DATASET                    |         |         |
  |   |   | City            |         | id      |
  |   |   |   | id          | integer |         | open
  |   |   |   | name        | string  |         | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

ID=6b1b4150-2aae-47b2-b28f-e750a28536e5
http POST "$SERVER/$DATASET/City" $AUTH _id=$ID id:=1 name=Vilnius
REV=$(http GET "$SERVER/$DATASET/City/$ID" | jq -r ._revision)
http PATCH "$SERVER/$DATASET/City/$ID" $AUTH _revision=$REV id:=2
REV=$(http GET "$SERVER/$DATASET/City/$ID" | jq -r ._revision)
http PATCH "$SERVER/$DATASET/City/$ID" $AUTH _revision=$REV id:=3

http GET "$SERVER/$DATASET/City/:changes?format(ascii)"
http GET "$SERVER/$DATASET/City/$ID/:changes?format(ascii)"

xdg-open http://localhost$SERVER/$DATASET/City/:changes
xdg-open http://localhost$SERVER/$DATASET/City/$ID/:changes
xdg-open http://localhost$SERVER/$DATASET/City/$ID
