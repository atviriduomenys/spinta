# 2023-02-17 13:24

git log -1 --oneline
#| 0ebfd06 (HEAD -> geometry-z, origin/master, origin/HEAD, master) Check if public data was written

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/geometry
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property | type                      | access
$DATASET                 |                           | 
  |   |   | Lake         |                           | 
  |   |   |   | name     | string                    | open
  |   |   |   | place    | geometry(point, 3346)     | open
  |   |   |   | shape    | geometry(3346)            | open
  |   |   | Lake3D       |                           | 
  |   |   |   | name     | string                    | open
  |   |   |   | place    | geometry(pointz, 3346)    | open
  |   |   |   | shape    | geometry(geometryz, 3346) | open
EOF

http -b https://epsg.io/3346.json | jq -r .name
#| LKS94 / Lithuania TM

# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations
# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server

psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Lake"'
#|                     Table "public.types/geometry/Lake"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  place     | geometry(Point,3346)        |           |          | 
#|  shape     | geometry(Geometry,3346)     |           |          | 

psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/Lake3D"'
#|                    Table "public.types/geometry/Lake3D"
#|   Column   |            Type             | Collation | Nullable | Default 
#| -----------+-----------------------------+-----------+----------+---------
#|  place     | geometry(PointZ,3346)       |           |          | 
#|  shape     | geometry(GeometryZ,3346)    |           |          | 

# notes/spinta/client.sh    Configure client

http POST "$SERVER/$DATASET/Lake" "$AUTH" \
    name="Victoria" \
    place="POINT (6237902 432224)" \
    shape="POLYGON ((
        6237680.75000000 432588.50000000,
        6237684.50000000 432585.62500000,
        6237682.50000000 432587.00000000,
        6237680.75000000 432588.50000000
    ))"
#| Geometry SRID (0) does not match column SRID (3346)


http POST "$SERVER/$DATASET/Lake" "$AUTH" \
    name="Victoria" \
    place="SRID=3346; POINT (6237902 432224)" \
    shape="SRID=3346; POLYGON ((
        6237680.75000000 432588.50000000,
        6237684.50000000 432585.62500000,
        6237682.50000000 432587.00000000,
        6237680.75000000 432588.50000000
    ))"
#| HTTP/1.1 201 Created


http POST "$SERVER/$DATASET/Lake" "$AUTH" \
    name="Victoria" \
    place="SRID=3346; POINT (6237902 432224)" \
    shape="SRID=3346; ((
        6237680.75000000 432588.50000000,
        6237684.50000000 432585.62500000,
        6237682.50000000 432587.00000000,
        6237680.75000000 432588.50000000
    ))"
#| parse error - invalid geometry


http POST "$SERVER/$DATASET/Lake" "$AUTH" \
    name="Victoria" \
    place="SRID=3346; POINT (6237902 432224 42)"
#| Geometry has Z dimension but column does not


http POST "$SERVER/$DATASET/Lake" "$AUTH" \
    name="Victoria" \
    shape="SRID=3346; POLYGON ((
        6237680.75000000 432588.50000000 42,
        6237684.50000000 432585.62500000 42,
        6237682.50000000 432587.00000000 42,
        6237680.75000000 432588.50000000 42
    ))"
#| Geometry has Z dimension but column does not

http POST "$SERVER/$DATASET/Lake3D" "$AUTH" \
    name="Victoria" \
    place="SRID=3346; POINT (6237902 432224 42)"
#| HTTP/1.1 201 Created


http POST "$SERVER/$DATASET/Lake3D" "$AUTH" \
    name="Victoria" \
    place="SRID=3346; POINT (6237902 432224)"
#| Column has Z dimension but geometry does not


http POST "$SERVER/$DATASET/Lake3D" "$AUTH" \
    name="Victoria" \
    shape="SRID=3346; POLYGON ((
        6237680.75000000 432588.50000000 42,
        6237684.50000000 432585.62500000 42,
        6237682.50000000 432587.00000000 42,
        6237680.75000000 432588.50000000 42
    ))"
#| HTTP/1.1 201 Created


http POST "$SERVER/$DATASET/Lake3D" "$AUTH" \
    name="Victoria" \
    shape="SRID=3346; POLYGON ((
        6237680.75000000 432588.50000000,
        6237684.50000000 432585.62500000,
        6237682.50000000 432587.00000000,
        6237680.75000000 432588.50000000
    ))"
#| Column has Z dimension but geometry does not



http GET "$SERVER/$DATASET/Lake?select(name,place)&format(ascii)"
#| --------  ----------------------
#| name      place
#| Victoria  POINT (6237902 432224)
#| --------  ----------------------


http GET "$SERVER/$DATASET/Lake?select(name,shape)&format(ascii)"
#| --------  --------------------------------------------------------------------------------------------
#| name      shape
#| Victoria  POLYGON ((6237680.75 432588.5, 6237684.5 432585.625, 6237682.5 432587, 6237680.75 432588.5))
#| --------  --------------------------------------------------------------------------------------------


http GET "$SERVER/$DATASET/Lake3D?select(name,place)&format(ascii)"
#| --------  ---------------------------
#| name      place
#| Victoria  POINT Z (6237902 432224 42)
#| --------  ---------------------------


http GET "$SERVER/$DATASET/Lake3D?select(name,shape)&shape!=null&format(ascii)"
#| --------  ----------------------------------------------------------------------------------------------------------
#| name      shape
#| Victoria  POLYGON Z ((6237680.75 432588.5 42, 6237684.5 432585.625 42, 6237682.5 432587 42, 6237680.75 432588.5 42))
#| --------  ----------------------------------------------------------------------------------------------------------
