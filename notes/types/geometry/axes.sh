# 2023-03-02 14:50

# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=types/geometry/axes
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server

cat > $BASEDIR/manifest.txt <<EOF
d | r | b | m | property         | type           | access
$DATASET                         |                | 
  |   |   | LKS94                |                | 
  |   |   |   | point            | geometry(3346) | open
  |   |   | WGS84                |                | 
  |   |   |   | point            | geometry(4326) | open
  |   |   | WGS84PseudoMercator  |                | 
  |   |   |   | point            | geometry(3857) | open
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show

http -b https://epsg.io/3346.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
http -b https://epsg.io/3346.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "LKS94 / Lithuania TM",
#|   "axis": [
#|     {"name": "Northing", "abbreviation": "X", "direction": "north", "unit": "metre"},
#|     {"name": "Easting",  "abbreviation": "Y", "direction": "east",  "unit": "metre"}
#|   ]
#| }

http -b https://epsg.io/4326.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "WGS 84",
#|   "axis": [
#|     {"name": "Geodetic latitude",  "abbreviation": "Lat", "direction": "north", "unit": "degree"},
#|     {"name": "Geodetic longitude", "abbreviation": "Lon", "direction": "east",  "unit": "degree"}
#|   ]
#| }

http -b https://epsg.io/3857.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "WGS 84 / Pseudo-Mercator",
#|   "axis": [
#|     {"name": "Easting",  "abbreviation": "X", "direction": "east",  "unit": "metre"},
#|     {"name": "Northing", "abbreviation": "Y", "direction": "north", "unit": "metre"}
#|   ]
#| }


# Bell tower of Vilnius Cathedral
#
# https://www.openstreetmap.org/?mlat=54.68570&mlon=25.28669#map=19/54.68570/25.28669
# 54.68570 N, 25.28669 E
#
# https://maps.lt/map/?lang=lt#obj=582966;6061790&xy=582981,6061783&z=1000&lrs=vector,hybrid_overlay,vector_2_5d,stops,zebra
# 582981 E, 6061783 N

# Rubikiai railway station
#
# https://www.openstreetmap.org/?mlat=55.53124&mlon=25.28203#map=18/55.53124/25.28203
# 55.53124 N, 25.28203 E
#
# https://maps.lt/map/?lang=lt#obj=580940;6155884&xy=580964,6155873&z=1000&lrs=vector,hybrid_overlay,vector_2_5d,stops,zebra
# 580964 E, 6155873 N

#                                  north     east
#                                  lat       lon
#                                  y         x
# 4326    WGS84                    54.68570  25.28669   Bell tower of Vilnius Cathedral
#                                  55.53124  25.28203   Rubikiai railway station
#
#                                  north     east
#                                  lat       lon
#                                  x         y
# 3346    LKS94                    6061792   582966     Bell tower of Vilnius Cathedral
#                                  6155873   580964     Rubikiai railway station
#
#                                  east      north
#                                  lat       lon
#                                  x         y
# 3857    WGS84 / Pseudo-Mercator  2814901   7301104    Bell tower of Vilnius Cathedral
#                                  2814382   7465659    Rubikiai railway station


poetry run python
from pyproj import CRS, Transformer
from shapely.ops import transform
from shapely.geometry import Point

def trans(x, y, s, t, **kw):
    transformer = Transformer.from_crs(
        crs_from=CRS(f'EPSG:{s}'),
        crs_to=CRS(f'EPSG:{t}'),
        **kw,
    )
    shape = transform(transformer.transform, Point(x, y))
    return shape.x, shape.y

# WGS84 (4326) -> LKS94 (3346) Bell tower of Vilnius Cathedral
trans(54.68570, 25.28669, 4326, 3346)                  # (north, east)
#| (6061789.991147185, 582964.0913465106)              # (north, east)
trans(25.28669, 54.68570, 4326, 3346, always_xy=True)  # (east, north)
#| (582964.0913465106, 6061789.991147185)              # (east, north)

# WGS84 (4326) -> LKS94 (3346) Rubikiai railway station
trans(55.53124, 25.28203, 4326, 3346)                  # (north, east)
#| (6155887.721039969, 580936.2572643314)              # (north, east)
trans(25.28203, 55.53124, 4326, 3346, always_xy=True)  # (east, north)
#| (580936.2572643314, 6155887.721039969)              # (east, north)


# WGS84 (4326) -> WGS84 / Pseudo-Mercator (3857) Bell tower of Vilnius Cathedral
trans(54.68570, 25.28669, 4326, 3857)                  # (north, east)
#| (2814901.454647363, 7301104.288392755)              # (east, north)
trans(25.28669, 54.68570, 4326, 3857, always_xy=True)  # (east, north)
#| (2814901.454647363, 7301104.288392755)              # (east, north)

# WGS84 (4326) -> WGS84 / Pseudo-Mercator (3857) Rubikiai railway station
trans(55.53124, 25.28203, 4326, 3857)                  # (north, east)
#| (2814382.705820266, 7465659.17820406)               # (east, north)
trans(25.28203, 55.53124, 4326, 3857, always_xy=True)  # (east, north)
#| (2814382.705820266, 7465659.17820406)               # (east, north)


CRS(f'EPSG:4326')
#| Name: WGS 84
#| Axis Info [ellipsoidal]:
#| - Lat[north]: Geodetic latitude (degree)
#| - Lon[east]: Geodetic longitude (degree)

CRS(f'EPSG:3346')
#| Name: LKS94 / Lithuania TM
#| Axis Info [cartesian]:
#| - X[north]: Northing (metre)
#| - Y[east]: Easting (metre)

CRS(f'EPSG:3857')
#| Name: WGS 84 / Pseudo-Mercator
#| Axis Info [cartesian]:
#| - X[east]: Easting (metre)
#| - Y[north]: Northing (metre)


# LKS94 (3346) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
trans(6061789, 582964, 3346, 4326)                  # (north, east)
#| (54.68569111173754, 25.286688302053335)          # (north, east)

# WGS84 / Pseudo-Mercator (3857) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
trans(2814901, 7301104, 3857, 4326)                 # (east, north)
#| (54.68569850243032, 25.28668591583325)           # (north, east)

exit()


# notes/spinta/server.sh    Prepare manifest
# notes/spinta/server.sh    Check configuration
# notes/spinta/server.sh    Run migrations

# notes/spinta/server.sh    Add client
# notes/spinta/server.sh    Run server
# notes/spinta/client.sh    Configure client

# LKS94 (3346) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
xdg-open http://localhost:8000/_srid/3346/6061789/582964
# WGS84 / Pseudo-Mercator (3857) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
xdg-open http://localhost:8000/_srid/3857/2814901/7301104
# WGS84 (4326) -> WGS84 (4326)  Bell tower of Vilnius Cathedral
xdg-open http://localhost:8000/_srid/4326/54.68569/25.28668


psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/LKS94"'
#| point     | geometry(Geometry,3346)     |           |          | 

http POST "$SERVER/$DATASET/LKS94" $AUTH point="POINT(6061789 582964)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "e77c0849-c1ce-4e27-8f91-237a4e53f07d",
#|     "_revision": "45559ce1-bee5-4622-b38a-55b54b27a783",
#|     "_type": "types/geometry/axes/LKS94",
#|     "point": "POINT(6061789 582964)"
#| }


psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/WGS84PseudoMercator"'
#| point     | geometry(Geometry,3857)     |           |          | 

http POST "$SERVER/$DATASET/WGS84PseudoMercator" $AUTH point="POINT(2814901 7301104)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "89ec85ac-bc99-4872-8393-d210d2def09c",
#|     "_revision": "6d93a50b-5012-4ef5-b6b6-1900ea2f23d4",
#|     "_type": "types/geometry/axes/WGS84PseudoMercator",
#|     "point": "POINT(2814901 7301104)"
#| }


psql -h localhost -p 54321 -U admin spinta -c '\d "'$DATASET'/WGS84"'
#| point     | geometry(Geometry,4326)     |           |          | 

http POST "$SERVER/$DATASET/WGS84" $AUTH point="POINT(54.68569 25.28668)"
#| HTTP/1.1 201 Created
#| 
#| {
#|     "_id": "e5cd2b1a-e11f-451a-9e4c-eaea6c3bf639",
#|     "_revision": "9bdf15cb-7d09-4371-a3fb-8f8a8f71f07e",
#|     "_type": "types/geometry/axes/WGS84",
#|     "point": "POINT(54.68569 25.28668)"
#| }


http GET "$SERVER/$DATASET/LKS94?select(point)&format(html)"
#| <a href="https://www.openstreetmap.org/
#|          ?mlat=  54.68569111173754&amp;mlon=25.286688302053335
#|          #map=19/54.68569111173754/         25.286688302053335">
#|          POINT ( 6061789                    582964)

http GET "$SERVER/$DATASET/WGS84PseudoMercator?select(point)&format(html)"
#| <a href="https://www.openstreetmap.org/
#|          ?mlat=  54.68569850243032&amp;mlon=25.28668591583325
#|          #map=19/54.68569850243032/         25.28668591583325">
#|          POINT ( 2814901                    7301104)

http GET "$SERVER/$DATASET/WGS84?select(point)&format(html)"
#| <a href="https://www.openstreetmap.org/
#|          ?mlat=  54.68569&amp;         mlon=25.28668
#|          #map=19/54.68569/                  25.28668">
#|          POINT ( 54.68569                   25.28668)
