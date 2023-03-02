# 2023-03-02 13:47

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/geometry
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
#|     "_id": "b59e19b2-3d68-4f35-8d9f-a53260563879",
#|     "_revision": "95ad26b3-0610-44d5-95f9-5ab8bb12de9d",
#|     "_type": "types/geometry/Point",
#|     "point": "POINT(54.69806 25.27386)"
#| }

http POST "$SERVER/$DATASET/PointLKS94" $AUTH point="SRID=4326;POINT(54.69806 25.27386)"
#| Geometry SRID (4326) does not match column SRID (3346)

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
#| Geometry SRID (0) does not match column SRID (3346)
# FIXME: As in description, SRID should not be required.

http POST "$SERVER/$DATASET/PointLKS94" $AUTH point="SRID=3346;POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "f83ff7d9-e099-4822-a579-651ee28108b5",
#|     "_revision": "ec04d623-0f9b-4c34-9c63-134ca664f838",
#|     "_type": "types/geometry/PointLKS94",
#|     "point": "SRID=3346;POINT(3687214 3185637)"
#| }

http POST "$SERVER/$DATASET/PointWGS84" $AUTH point="POINT(54.69806 25.27386)"
#| Geometry SRID (0) does not match column SRID (4326)
# FIXME: As in description, SRID should not be required.

http POST "$SERVER/$DATASET/PointWGS84" $AUTH point="SRID=4326;POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "be47ebbd-4e85-4252-aa46-d80cc0363374",
#|     "_revision": "6127d557-17e7-4b82-879f-bacf789e75de",
#|     "_type": "types/geometry/PointWGS84",
#|     "point": "SRID=4326;POINT(54.69806 25.27386)"
#| }

http GET "$SERVER/$DATASET/Point?select(point)&format(ascii)"
#| -------------------------
#| point
#| POINT (54.69806 25.27386)
#| -------------------------

http GET "$SERVER/$DATASET/PointLKS94?select(point)&format(ascii)"
#| -----------------------
#| point
#| POINT (3687214 3185637)
#| -----------------------

http GET "$SERVER/$DATASET/PointWGS84?select(point)&format(ascii)"
#| -------------------------
#| point
#| POINT (54.69806 25.27386)
#| -------------------------
