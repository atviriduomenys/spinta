# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=query/id
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type   | ref  | access
$DATASET                 |        |      |
  |   |   | City         |        | name |
  |   |   |   | name     | string |      | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/City" $AUTH _id=9dc732fb-7df2-454c-8bf1-04e35d5f651b name=Vilnius
http POST "$SERVER/$DATASET/City" $AUTH _id=a53a8e8a-440c-4a9d-9099-7061418f7803 name=Kaunas
http POST "$SERVER/$DATASET/City" $AUTH _id=168cd6a0-1962-4ed1-b11a-1653dd6812f7 name=Klaidpėda
http POST "$SERVER/$DATASET/City" $AUTH _id=44ef76d0-74f8-4179-82e8-08e4a83f6cf4 name=Šiauliai
http POST "$SERVER/$DATASET/City" $AUTH _id=0178310e-06aa-404d-ac67-1acfaf6a9751 name=Panevėžys

http GET "$SERVER/$DATASET/City?select(_id,name)&sort(_id)&format(ascii)"
#| _id                                   name    
#| ------------------------------------  ---------
#| 0178310e-06aa-404d-ac67-1acfaf6a9751  Panevėžys
#| 168cd6a0-1962-4ed1-b11a-1653dd6812f7  Klaidpėda
#| 44ef76d0-74f8-4179-82e8-08e4a83f6cf4  Šiauliai
#| 9dc732fb-7df2-454c-8bf1-04e35d5f651b  Vilnius
#| a53a8e8a-440c-4a9d-9099-7061418f7803  Kaunas

http GET "$SERVER/$DATASET/City?_id>'44ef76d0-74f8-4179-82e8-08e4a83f6cf4'&select(_id,name)&sort(_id)&format(ascii)"
#| _id                                   name   
#| ------------------------------------  -------
#| 9dc732fb-7df2-454c-8bf1-04e35d5f651b  Vilnius
#| a53a8e8a-440c-4a9d-9099-7061418f7803  Kaunas
