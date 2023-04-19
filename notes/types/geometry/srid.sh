# 2023-03-02 13:47

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/geometry/srid
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type           | access
$DATASET                 |                | 
  |   |   | Point        |                | 
  |   |   |   | name     | string         | open
  |   |   |   | point    | geometry       | open
  |   |   | PointLKS94   |                | 
  |   |   |   | name     | string         | open
  |   |   |   | point    | geometry(3346) | open
  |   |   | PointWGS84   |                | 
  |   |   |   | name     | string         | open
  |   |   |   | point    | geometry(4326) | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'

# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http DELETE "$SERVER/$DATASET/:wipe" $AUTH

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Point"'
#| point     | geometry                    |           |          | 

http POST "$SERVER/$DATASET/Point" $AUTH \
    _id="5e056d51-eb75-45c8-877b-6c9f287427d9" \
    name="Point 1" \
    point="POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "5e056d51-eb75-45c8-877b-6c9f287427d9",
#|     "_revision": "6a0aa105-1f25-4bca-bf1c-d83a521d4b1e",
#|     "_type": "types/geometry/axes/Point",
#|     "point": "POINT(54.69806 25.27386)"
#| }

http GET "$SERVER/$DATASET/Point?select(name,point)&format(ascii)"
#| name     point          
#| -------  -------------------------
#| Point 1  POINT (54.69806 25.27386)

http PATCH "$SERVER/$DATASET/Point/5e056d51-eb75-45c8-877b-6c9f287427d9" $AUTH \
    _revision="6a0aa105-1f25-4bca-bf1c-d83a521d4b1e" \
    point="POINT(54.69807 25.27387)"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "5e056d51-eb75-45c8-877b-6c9f287427d9",
#|     "_revision": "5174cecc-f1dd-419a-b8dd-883522f81f25",
#|     "_type": "types/geometry/srid/Point",
#|     "point": "POINT(54.69807 25.27387)"
#| }

http PATCH "$SERVER/$DATASET/Point/5e056d51-eb75-45c8-877b-6c9f287427d9" $AUTH \
    _revision="5174cecc-f1dd-419a-b8dd-883522f81f25" \
    name="Point 2"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "5e056d51-eb75-45c8-877b-6c9f287427d9",
#|     "_revision": "0fba8a1e-0673-4ea1-8d06-7ae5809a4a4b",
#|     "_type": "types/geometry/srid/Point",
#|     "name": "Point 2"
#| }

http GET "$SERVER/$DATASET/Point?select(name,point)&format(ascii)"
#| name     point          
#| -------  -------------------------
#| Point 2  POINT (54.69807 25.27387)

http DELETE "$SERVER/$DATASET/Point/5e056d51-eb75-45c8-877b-6c9f287427d9" $AUTH
#| HTTP/1.1 204 No Content


http GET "$SERVER/$DATASET/Point?select(name,point)&format(ascii)"
# (empty)



http -b https://epsg.io/3346.json | jq -r .name
#| LKS94 / Lithuania TM

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/PointLKS94"'
#| point     | geometry(Geometry,3346)     |           |          | 

# Try to pass incorrect SRID
http POST "$SERVER/$DATASET/PointLKS94" $AUTH point="SRID=4326;POINT(54.69806 25.27386)"
#| Geometry SRID (4326) does not match column SRID (3346)
# TODO: This should be a 400 error, not 500.
#       https://github.com/atviriduomenys/spinta/issues/392

psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(
    st_transform(
        'SRID=4326;POINT(54.69806 25.27386)'::geometry,
        3346
    )
)
EOF
#| POINT(3687214.32818431 3185637.15841001)

http POST "$SERVER/$DATASET/PointLKS94" $AUTH \
    _id="4f3541d5-a5b2-4dec-8ca4-143bd4c93789" \
    name="Point 1" \
    point="POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "4f3541d5-a5b2-4dec-8ca4-143bd4c93789",
#|     "_revision": "665837bc-0089-4fda-9fc5-16e052a363b9",
#|     "_type": "types/geometry/srid/PointLKS94",
#|     "name": "Point 1",
#|     "point": "POINT(3687214 3185637)"
#| }

http POST "$SERVER/$DATASET/PointLKS94" $AUTH \
    _id="d44ec71e-6e10-4073-b226-b7a298b9b512" \
    name="Point 2" \
    point="SRID=3346;POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "d44ec71e-6e10-4073-b226-b7a298b9b512",
#|     "_revision": "ec68f4d4-27d4-4c96-8add-a5fbcf6987f4",
#|     "_type": "types/geometry/srid/PointLKS94",
#|     "name": "Point 2",
#|     "point": "SRID=3346;POINT(3687214 3185637)"
#| }
# TODO: I think, SRID should not be here.

http GET "$SERVER/$DATASET/PointLKS94?select(name,point)&format(ascii)"
#| name     point
#| -------  -----------------------
#| Point 1  POINT (3687214 3185637)
#| Point 2  POINT (3687214 3185637)

http PATCH "$SERVER/$DATASET/PointLKS94/4f3541d5-a5b2-4dec-8ca4-143bd4c93789" $AUTH \
    _revision="665837bc-0089-4fda-9fc5-16e052a363b9" \
    name="Point 1.1"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "4f3541d5-a5b2-4dec-8ca4-143bd4c93789",
#|     "_revision": "11e77fbb-86f0-487a-b573-56625d36bf9a",
#|     "_type": "types/geometry/srid/PointLKS94",
#|     "name": "Point 1.1"
#| }

http PATCH "$SERVER/$DATASET/PointLKS94/4f3541d5-a5b2-4dec-8ca4-143bd4c93789" $AUTH \
    _revision="11e77fbb-86f0-487a-b573-56625d36bf9a" \
    point="POINT(3687211 3185611)"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "4f3541d5-a5b2-4dec-8ca4-143bd4c93789",
#|     "_revision": "676bb05d-c289-4441-934c-712e4fc625da",
#|     "_type": "types/geometry/srid/PointLKS94",
#|     "point": "POINT(3687211 3185611)"
#| }

http GET "$SERVER/$DATASET/PointLKS94?select(name,point)&format(ascii)"
#| name       point
#| ---------  -----------------------
#| Point 1.1  POINT (3687211 3185611)
#| Point 2    POINT (3687214 3185637)

http DELETE "$SERVER/$DATASET/PointLKS94/4f3541d5-a5b2-4dec-8ca4-143bd4c93789" $AUTH
#| HTTP/1.1 204 No Content

http GET "$SERVER/$DATASET/PointLKS94?select(name,point)&format(ascii)"
#| name       point
#| ---------  -----------------------
#| Point 2    POINT (3687214 3185637)



http -b https://epsg.io/4326.json | jq -r .name
#| WGS 84

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/PointWGS84"'
#| point     | geometry(Geometry,4326)     |           |          | 

http POST "$SERVER/$DATASET/PointWGS84" $AUTH \
    _id="56be1676-d32e-4fcf-a2f6-2ea933707dc0" \
    name="Point 1" \
    point="POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "56be1676-d32e-4fcf-a2f6-2ea933707dc0",
#|     "_revision": "a9d54711-8176-42fc-95b1-1307a42e51a4",
#|     "_type": "types/geometry/axes/PointWGS84",
#|     "point": "POINT(54.69806 25.27386)"
#| }

http POST "$SERVER/$DATASET/PointWGS84" $AUTH \
    _id="627c98e5-a998-47da-9adb-ac4e4dad71eb" \
    name="Point 2" \
    point="SRID=4326;POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "627c98e5-a998-47da-9adb-ac4e4dad71eb",
#|     "_revision": "653c9416-6c32-4c18-8358-5f73609cc8e6",
#|     "_type": "types/geometry/axes/PointWGS84",
#|     "point": "SRID=4326;POINT(54.69806 25.27386)"
#| }
# TODO: I think, SRID should not be here.

http GET "$SERVER/$DATASET/PointWGS84?select(name,point)&format(ascii)"
#| name     point
#| -------  -------------------------
#| Point 1  POINT (54.69806 25.27386)
#| Point 2  POINT (54.69806 25.27386)

http PATCH "$SERVER/$DATASET/PointWGS84/56be1676-d32e-4fcf-a2f6-2ea933707dc0" $AUTH \
    _revision="a9d54711-8176-42fc-95b1-1307a42e51a4" \
    name="Point 1.1"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "56be1676-d32e-4fcf-a2f6-2ea933707dc0",
#|     "_revision": "a22e6eac-05f1-4342-8d18-1281ad64c586",
#|     "_type": "types/geometry/srid/PointWGS84",
#|     "name": "Point 1.1"
#| }

http PATCH "$SERVER/$DATASET/PointWGS84/56be1676-d32e-4fcf-a2f6-2ea933707dc0" $AUTH \
    _revision="a22e6eac-05f1-4342-8d18-1281ad64c586" \
    point="POINT(54.69111 25.27111)"
#| HTTP/1.1 200 OK
#| 
#| {
#|     "_id": "56be1676-d32e-4fcf-a2f6-2ea933707dc0",
#|     "_revision": "46979db7-4856-431a-b837-196039ae19e5",
#|     "_type": "types/geometry/srid/PointWGS84",
#|     "point": "POINT(54.69111 25.27111)"
#| }

http GET "$SERVER/$DATASET/PointWGS84?select(name,point)&format(ascii)"
#| name       point
#| ---------  -------------------------
#| Point 1.1  POINT (54.69111 25.27111)
#| Point 2    POINT (54.69806 25.27386)

http GET "$SERVER/$DATASET/PointWGS84?select(name,point)&format(json)"
#| {
#|     "_data": [
#|         {"name": "Point 2", "point": "POINT (54.69806 25.27386)"},
#|         {"name": "Point 1.1", "point": "POINT (54.69111 25.27111)"}
#|     ]
#| }

http DELETE "$SERVER/$DATASET/PointWGS84/56be1676-d32e-4fcf-a2f6-2ea933707dc0" $AUTH
#| HTTP/1.1 204 No Content

http GET "$SERVER/$DATASET/PointWGS84?select(name,point)&format(ascii)"
#| name     point
#| -------  -------------------------
#| Point 2  POINT (54.69806 25.27386)


http GET "$SERVER/$DATASET/:all?select(point)&format(ascii)"
#| HTTP/1.1 500 Internal Server Error
#| 
#| {
#|     "errors": [
#|         {
#|             "code": "NotImplementedError",
#|             "message": "Could not find signature for render: <Context, Request, Namespace, Ascii>"
#|         }
#|     ]
#| }
tail -50 $BASEDIR/spinta.log
#| Traceback (most recent call last):
#|   File "spinta/api.py", line 94, in homepage
#|     return await create_http_response(context, params, request)
#|   File "spinta/utils/response.py", line 153, in create_http_response
#|     return await commands.getall(
#|   File "spinta/types/namespace.py", line 189, in getall
#|     return render(context, request, ns, params, rows, action=action)
#|   File "spinta/renderer.py", line 23, in render
#|     return commands.render(
#|   File "multipledispatch/dispatcher.py", line 273, in __call__
#|     raise NotImplementedError(
#| NotImplementedError: Could not find signature for render: <Context, Request, Namespace, Ascii>
# TODO: Implement ascii for namespaces
#       https://github.com/atviriduomenys/spinta/issues/393
