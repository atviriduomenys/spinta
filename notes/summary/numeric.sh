# 2023-08-03 16:31

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=summary/numeric
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                  | ref       | access
$DATASET                 |                       |           | 
  |   |   | Event        |                       |           | 
  |   |   |   | type     | ref                   | EventType | open
  |   |   |   | title    | string                |           | open
  |   |   |   | date     | date                  |           | open
  |   |   |   | tickets  | integer               |           | open
  |   |   |   | place    | geometry(point, 3346) |           | open
  |   |   | EventType    |                       |           | 
  |   |   |   | title    | string                |           | open
EOF

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

uuidgen
ET1=f755b7ba-102c-4faa-9402-3fad51c915f2
http POST "$SERVER/$DATASET/EventType" $AUTH _id=$ET1 title="ET1"
ET2=134631eb-e52e-491b-a052-64c37bfb41cf
http POST "$SERVER/$DATASET/EventType" $AUTH _id=$ET2 title="ET2"

uuidgen
E1=8ac44a6d-ef9d-49e6-abc1-d73796cdb0ec
http POST "$SERVER/$DATASET/Event" $AUTH _id=$E1 type'[_id]'=$ET1 title="E1" date=2023-08-03 tickets:=100 place="POINT (432224 6237902)"
E2=b2fc3abe-3e0c-42e9-ac70-2bde618bce84
http POST "$SERVER/$DATASET/Event" $AUTH _id=$E2 type'[_id]'=$ET2 title="E2" date=2023-08-05 tickets:=108 place="POINT (432324 6257902)"
E3=23efefdd-b305-48c9-8d28-25353f65247e
http POST "$SERVER/$DATASET/Event" $AUTH _id=$E3 type'[_id]'=$ET1 title="E3" date=2023-09-01 tickets:=99 place="POINT (412324 6247902)"

http GET "$SERVER/$DATASET/Event/:summary"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "Exception",
#|             "message": "At least 1 argument is required for 'summary' URL parameter."
# TODO: Should be a better error message and instead of Exception a more
#       specific error code should be given.
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Event/:summary/tickets"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "23efefdd-b305-48c9-8d28-25353f65247e",
#|             "_type": "summary/numeric/Event",
#|             "bin": 99.045,
#|             "count": 1
#|         },
#|         {
#|             "_type": "summary/numeric/Event",
#|             "bin": 99.135,
#|             "count": 0
#|         },
#|         ...
#|         {
#|             "_id": "8ac44a6d-ef9d-49e6-abc1-d73796cdb0ec",
#|             "_type": "summary/numeric/Event",
#|             "bin": 100.035,
#|             "count": 1
#|         },
#|         ...
#|         {
#|             "_id": "b2fc3abe-3e0c-42e9-ac70-2bde618bce84",
#|             "_type": "summary/numeric/Event",
#|             "bin": 107.955,
#|             "count": 1
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Event/:summary/type"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_type": "summary/numeric/Event",
#|             "bin": "f755b7ba",
#|             "count": 2
#|         },
#|         {
#|             "_id": "b2fc3abe-3e0c-42e9-ac70-2bde618bce84",
#|             "_type": "summary/numeric/Event",
#|             "bin": "134631eb",
#|             "count": 1
#|         }
#|     ]
#| }


http GET "$SERVER/$DATASET/Event/:summary/date"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "8ac44a6d-ef9d-49e6-abc1-d73796cdb0ec",
#|             "_type": "summary/numeric/Event",
#|             "bin": "2023-08-03 03:28:48",
#|             "count": 1
#|         },
#|         {
#|             "_type": "summary/numeric/Event",
#|             "bin": "2023-08-03 10:26:24",
#|             "count": 0
#|         },
#|         ...
#|         {
#|             "_id": "b2fc3abe-3e0c-42e9-ac70-2bde618bce84",
#|             "_type": "summary/numeric/Event",
#|             "bin": "2023-08-04 21:14:24",
#|             "count": 1
#|         },
#|         ...
#|         {
#|             "_id": "23efefdd-b305-48c9-8d28-25353f65247e",
#|             "_type": "summary/numeric/Event",
#|             "bin": "2023-08-31 20:31:12",
#|             "count": 1
#|         }
#|     ]
#| }

http GET "$SERVER/$DATASET/Event/:summary/place"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_data": [
#|         {
#|             "_id": "8ac44a6d-ef9d-49e6-abc1-d73796cdb0ec",
#|             "_type": "summary/numeric/Event",
#|             "centroid": "POINT(432224 6237902)",
#|             "cluster": 1
#|         },
#|         {
#|             "_id": "b2fc3abe-3e0c-42e9-ac70-2bde618bce84",
#|             "_type": "summary/numeric/Event",
#|             "centroid": "POINT(432324 6257902)",
#|             "cluster": 1
#|         },
#|         {
#|             "_id": "23efefdd-b305-48c9-8d28-25353f65247e",
#|             "_type": "summary/numeric/Event",
#|             "centroid": "POINT(412324 6247902)",
#|             "cluster": 1
#|         }
#|     ]
#| }
