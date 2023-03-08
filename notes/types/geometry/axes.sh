# 2023-03-02 14:50

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/geometry/axes
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type           | access
$DATASET                 |                | 
  |   |   | LKS94        |                | 
  |   |   |   | point    | geometry(3346) | open
  |   |   | WGS84        |                | 
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

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/LKS94"'
#| point     | geometry(Geometry,3346)     |           |          | 
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/WGS84"'
#| point     | geometry(Geometry,4326)     |           |          | 

# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(
    st_transform(
        'SRID=4326;POINT(54.69806 25.27386)'::geometry,
        3346
    )
)
EOF
#| POINT(3687214.32818431 3185637.15841001)

http POST "$SERVER/$DATASET/LKS94" $AUTH point="SRID=3346;POINT(3687214 3185637)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "dfe3740a-29d6-4d55-a534-43218ee99763",
#|     "_revision": "55ebc81e-0a7a-452b-97b3-caec0bb6f19a",
#|     "_type": "types/geometry/axes/LKS94",
#|     "point": "SRID=3346;POINT(3687214 3185637)"
#| }

http POST "$SERVER/$DATASET/WGS84" $AUTH point="SRID=4326;POINT(54.69806 25.27386)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "422ab632-c98e-41e4-982e-9ad0bc6101ed",
#|     "_revision": "63abeeeb-575c-47cb-9cae-9b6fcf4e7cda",
#|     "_type": "types/geometry/axes/WGS84",
#|     "point": "SRID=4326;POINT(54.69806 25.27386)"
#| }

http GET "$SERVER/$DATASET/LKS94?select(point)&format(html)"
#| <a href="https://www.openstreetmap.org/?mlat=54.698056856776454&amp;mlon=25.27385941740757#map=19/54.698056856776454/25.27385941740757">
#|     POINT (3687214 3185637)
#| </a>

http GET "$SERVER/$DATASET/WGS84?select(point)&format(html)"
#| <a href="https://www.openstreetmap.org/?mlat=54.69806&amp;mlon=25.27386#map=19/54.69806/25.27386">
#|     POINT (54.69806 25.27386)
#| </a>
