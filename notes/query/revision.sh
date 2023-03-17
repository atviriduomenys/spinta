# 2023-03-02 13:23

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=query/revision
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

http POST "$SERVER/$DATASET/City" $AUTH name="Vilnius"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "08b3fa84-4883-4075-97f2-64e708bf1db2",
#|     "_revision": "05514443-7e5e-4880-9e43-8fdd3298d724",
#|     "_type": "query/revision/City",
#|     "name": "Vilnius"
#| }

http POST "$SERVER/$DATASET/City" $AUTH name="Kaunas"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "ed04e26a-effe-44a7-addd-70089202735d",
#|     "_revision": "0f5940c9-12ef-4ab7-8041-d4986e3a4959",
#|     "_type": "query/revision/City",
#|     "name": "Kaunas"
#| }

http GET "$SERVER/$DATASET/City?format(ascii)"
#| -------------------  ------------------------------------  ------------------------------------  -------
#| _type                _id                                   _revision                             name
#| query/revision/City  08b3fa84-4883-4075-97f2-64e708bf1db2  05514443-7e5e-4880-9e43-8fdd3298d724  Vilnius
#| query/revision/City  ed04e26a-effe-44a7-addd-70089202735d  0f5940c9-12ef-4ab7-8041-d4986e3a4959  Kaunas
#| -------------------  ------------------------------------  ------------------------------------  -------

http GET "$SERVER/$DATASET/City?select(_id,_revision,name)&format(ascii)"
#| ------------------------------------  ------------------------------------  -------
#| _id                                   _revision                             name
#| 08b3fa84-4883-4075-97f2-64e708bf1db2  05514443-7e5e-4880-9e43-8fdd3298d724  Vilnius
#| ed04e26a-effe-44a7-addd-70089202735d  0f5940c9-12ef-4ab7-8041-d4986e3a4959  Kaunas
#| ------------------------------------  ------------------------------------  -------

http GET "$SERVER/$DATASET/City?select(_revision)&format(ascii)"
#| ------------------------------------
#| _revision
#| 05514443-7e5e-4880-9e43-8fdd3298d724
#| 0f5940c9-12ef-4ab7-8041-d4986e3a4959
#| ------------------------------------
#
http GET "$SERVER/$DATASET/City?select(_id,_revision,name)&format(jsonl)"
#| HTTP/1.1 200 OK
#| content-type: application/x-json-stream
#| 
#| {"_id":"08b3fa84-4883-4075-97f2-64e708bf1db2","_revision":"05514443-7e5e-4880-9e43-8fdd3298d724","name":"Vilnius"}
#| {"_id":"ed04e26a-effe-44a7-addd-70089202735d","_revision":"0f5940c9-12ef-4ab7-8041-d4986e3a4959","name":"Kaunas"}
