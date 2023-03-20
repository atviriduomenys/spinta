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
  |   |   |   | point    | geometry       | open
  |   |   | PointLKS94   |                | 
  |   |   |   | point    | geometry(3346) | open
  |   |   | PointWGS84   |                | 
  |   |   |   | point    | geometry(4326) | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

http -b https://epsg.io/3346.json | jq -r .name
#| LKS94 / Lithuania TM

http -b https://epsg.io/4326.json | jq -r .name
#| WGS 84

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations

psql -h localhost -p 54321 -U admin spinta -c '\dt "'$DATASET'"*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Point"'
#| point     | geometry                    |           |          | 
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/PointLKS94"'
#| point     | geometry(Geometry,3346)     |           |          | 
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/PointWGS84"'
#| point     | geometry(Geometry,4326)     |           |          | 

# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Point" $AUTH point="POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "339eeee0-0282-456b-be36-418e5a405c17",
#|     "_revision": "eb2e25d7-f908-4a62-ae81-1614407a6968",
#|     "_type": "types/geometry/Point",
#|     "point": "POINT(54.69806 25.27386)"
#| }

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

http POST "$SERVER/$DATASET/PointLKS94" $AUTH point="POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "0d944689-62a9-416b-b390-ed7118a3e7f8",
#|     "_revision": "558770a7-2dd9-4882-8653-875732fb3461",
#|     "_type": "types/geometry/PointLKS94",
#|     "point": "POINT(3687214 3185637)"
#| }

http POST "$SERVER/$DATASET/PointLKS94" $AUTH point="SRID=3346;POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "f83ff7d9-e099-4822-a579-651ee28108b5",
#|     "_revision": "ec04d623-0f9b-4c34-9c63-134ca664f838",
#|     "_type": "types/geometry/PointLKS94",
#|     "point": "SRID=3346;POINT(3687214 3185637)"
#| }
# TODO: I think, SRID should not be here.

http POST "$SERVER/$DATASET/PointWGS84" $AUTH point="POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "b26a2027-650f-4de3-9758-3b4f8540683f",
#|     "_revision": "fd81c84c-17be-4c9c-9a24-f2ffed962d51",
#|     "_type": "types/geometry/PointWGS84",
#|     "point": "POINT(54.69806 25.27386)"
#| }

http POST "$SERVER/$DATASET/PointWGS84" $AUTH point="SRID=4326;POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "be47ebbd-4e85-4252-aa46-d80cc0363374",
#|     "_revision": "6127d557-17e7-4b82-879f-bacf789e75de",
#|     "_type": "types/geometry/PointWGS84",
#|     "point": "SRID=4326;POINT(54.69806 25.27386)"
#| }
# TODO: I think, SRID should not be here.

http GET "$SERVER/$DATASET/Point?select(point)&format(ascii)"
#| point
#| -------------------------
#| POINT (54.69806 25.27386)

http GET "$SERVER/$DATASET/PointLKS94?select(point)&format(ascii)"
#| point
#| -----------------------
#| POINT (3687214 3185637)
#| POINT (3687214 3185637)

http GET "$SERVER/$DATASET/PointWGS84?select(point)&format(ascii)"
#| point
#| -------------------------
#| POINT (54.69806 25.27386)
#| POINT (54.69806 25.27386)

http GET "$SERVER/$DATASET/PointWGS84?select(point)&format(json)"
#| {
#|     "_data": [
#|         {"point": "POINT (54.69806 25.27386)"},
#|         {"point": "POINT (54.69806 25.27386)"}
#|     ]
#| }

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
