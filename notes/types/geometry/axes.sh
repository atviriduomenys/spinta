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

http -b https://epsg.io/3346.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "LKS94 / Lithuania TM",
#|   "axis": [
#|     {
#|       "name": "Northing",
#|       "abbreviation": "X",
#|       "direction": "north",
#|       "unit": "metre"
#|     },
#|     {
#|       "name": "Easting",
#|       "abbreviation": "Y",
#|       "direction": "east",
#|       "unit": "metre"
#|     }
#|   ]
#| }

http -b https://epsg.io/4326.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "WGS 84",
#|   "axis": [
#|     {
#|       "name": "Geodetic latitude",
#|       "abbreviation": "Lat",
#|       "direction": "north",
#|       "unit": "degree"
#|     },
#|     {
#|       "name": "Geodetic longitude",
#|       "abbreviation": "Lon",
#|       "direction": "east",
#|       "unit": "degree"
#|     }
#|   ]
#| }

http -b https://epsg.io/3857.json | jq -r "{name: .name, axis: .coordinate_system.axis}"
#| {
#|   "name": "WGS 84 / Pseudo-Mercator",
#|   "axis": [
#|     {
#|       "name": "Easting",
#|       "abbreviation": "X",
#|       "direction": "east",
#|       "unit": "metre"
#|     },
#|     {
#|       "name": "Northing",
#|       "abbreviation": "Y",
#|       "direction": "north",
#|       "unit": "metre"
#|     }
#|   ]
#| }


# Katedros varpinė
#
# https://www.openstreetmap.org/?mlat=54.68570&mlon=25.28669#map=19/54.68570/25.28669
# 54.68570 N, 25.28669 E
#
# https://maps.lt/map/?lang=lt#obj=582966;6061790&xy=582981,6061783&z=1000&lrs=vector,hybrid_overlay,vector_2_5d,stops,zebra
# 582981 E, 6061783 N

# Rubikių geležinkelio stotis
#
# https://www.openstreetmap.org/?mlat=55.53124&mlon=25.28203#map=18/55.53124/25.28203
# 55.53124 N, 25.28203 E
#
# https://maps.lt/map/?lang=lt#obj=580940;6155884&xy=580964,6155873&z=1000&lrs=vector,hybrid_overlay,vector_2_5d,stops,zebra
# 580964 E, 6155873 N


#                                  north     east
#                                  lat       lon
#                                  y         x
# 4326    WGS84                    54.68570  25.28669   Katedros varpinė
#                                  55.53124  25.28203   Rubikių stotis
# 3346    LKS94                    6061792   582966     Katedros varpinė
#                                  6155873   580964     Rubikių stotis
#
#                                  east      north
#                                  lat       lon
#                                  x         y
# 3857    WGS84 / Pseudo-Mercator  2814901   7301104    Katedros varpinė
#                                  2814382   7465659    Rubikių stotis


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

# WGS84 (4326) -> LKS94 (3346) Katedros varpinė
trans(54.68570, 25.28669, 4326, 3346)                  # (north, east)
#| (6061789.991147185, 582964.0913465106)              # (north, east)
trans(25.28669, 54.68570, 4326, 3346, always_xy=True)  # (east, north)
#| (582964.0913465106, 6061789.991147185)              # (east, north)

# WGS84 (4326) -> LKS94 (3346) Rubikių stotis
trans(55.53124, 25.28203, 4326, 3346)                  # (north, east)
#| (6155887.721039969, 580936.2572643314)              # (north, east)
trans(25.28203, 55.53124, 4326, 3346, always_xy=True)  # (east, north)
#| (580936.2572643314, 6155887.721039969)              # (east, north)


# WGS84 (4326) -> WGS84 / Pseudo-Mercator (3857) Katedros varpinė
trans(54.68570, 25.28669, 4326, 3857)                  # (north, east)
#| (2814901.454647363, 7301104.288392755)              # (east, north)
trans(25.28669, 54.68570, 4326, 3857, always_xy=True)  # (east, north)
#| (2814901.454647363, 7301104.288392755)              # (east, north)

# WGS84 (4326) -> WGS84 / Pseudo-Mercator (3857) Rubikių stotis
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


# LKS94 (3346) -> WGS84 (4326)  Katedros varpinė
trans(6061789, 582964, 3346, 4326)                  # (north, east)
#| (54.68569111173754, 25.286688302053335)          # (north, east)


# WGS84 / Pseudo-Mercator (3857) -> WGS84 (4326)  Katedros varpinė
trans(2814901, 7301104, 3857, 4326)                 # (east, north)
#| (54.68569850243032, 25.28668591583325)           # (north, east)





trans(24.884, 56.423, 4326, 3346, always_xy=True)
#| (554539.5211979866, 6254761.641923006)

trans(6254761, 554539, 3346, 4326)  # PROJ swaps axes
#| (56.422994294771925, 24.883991419600004)

trans(554539, 6254761, 3346, 4326, always_xy=True)
#| (24.883991419600004, 56.422994294771925)

trans(6254761, 554539, 3346, 900913)  # PROJ swaps axes
#| (2770073.253734061, 7643086.849722873)

trans(554539, 6254761, 3346, 900913, always_xy=True)
#| (2770073.253734061, 7643086.849722873)

trans(6254761, 554539, 3346, 3857)  # PROJ swaps axes
#| (2770073.253734061, 7643086.849722873)

trans(554539, 6254761, 3346, 3857, always_xy=True)
#| (2770073.253734061, 7643086.849722873)





#                                  north   east
#                                  lat     lon
#                                  x       y
# 4326    WGS84                    24.884  56.423
# 3346    LKS94                    554539  6254761
# 900913  WebMerkator              2770074 7643087
# 3857    WGS84 / Pseudo-Merkator  2770074 7643087


#                                  north   east
#                                  lat     lon
#                                  x       y
# 4326    WGS84                    24.884  56.423
# 3346    LKS94                    554539  6254761
# 900913  WebMerkator              2770074 7643087
# 3857    WGS84 / Pseudo-Merkator  2770074 7643087


http "https://epsg.io/trans?x=24.884&y=56.423&z=8&s_srs=4326&t_srs=3346"
#| {
#|     "x": "554539.5211979866",
#|     "y": "6254761.641923006",
#|     "z": "8.0"
#| }

http "https://epsg.io/trans?x=554539&y=6254761&z=8&s_srs=3346&t_srs=4326"
#|{
#|    "x": "24.883991419600004",
#|    "y": "56.422994294771925",
#|    "z": "8.0"
#|}

http "https://epsg.io/trans?x=24.884&y=56.423&z=8&s_srs=4326&t_srs=900913"
#| {
#|     "x": "2770074.2088998198",
#|     "y": "7643087.99807262",
#|     "z": "8.0"
#| }

http "https://epsg.io/trans?x=24.884&y=56.423&z=8&s_srs=4326&t_srs=3857"
#| {
#|     "x": "2770074.2088998198",
#|     "y": "7643087.99807262",
#|     "z": "8.0"
#| }

http "https://epsg.io/trans?x=554539&y=6254761&z=8&s_srs=3346&t_srs=3857"
#| {
#|     "x": "2770073.253734061",
#|     "y": "7643086.849722873",
#|     "z": "8.0"
#| }


psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(st_transform('SRID=4326;POINT(24.884 56.423)'::geometry, 3346))
EOF
#| POINT(554539.521197987 6254761.64192301)

psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(st_transform('SRID=3346;POINT(554539 6254761)'::geometry, 4326))
EOF
#| POINT(24.8839914196 56.4229942947719)

psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(st_transform('SRID=3346;POINT(554539 6254761)'::geometry, 900913))
EOF
#| POINT(2770073.25373406 7643086.84954803)

psql -h localhost -p 54321 -U admin spinta <<'EOF'
select st_astext(st_transform('SRID=3346;POINT(554539 6254761)'::geometry, 3857))
EOF
#| POINT(2770073.25373406 7643086.84954803)


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

trans(56.423, 24.884, 4326, 3346)  # PROJ swaps axes
#| (6254761.641923006, 554539.5211979866)

trans(24.884, 56.423, 4326, 3346, always_xy=True)
#| (554539.5211979866, 6254761.641923006)

trans(6254761, 554539, 3346, 4326)  # PROJ swaps axes
#| (56.422994294771925, 24.883991419600004)

trans(554539, 6254761, 3346, 4326, always_xy=True)
#| (24.883991419600004, 56.422994294771925)

trans(6254761, 554539, 3346, 900913)  # PROJ swaps axes
#| (2770073.253734061, 7643086.849722873)

trans(554539, 6254761, 3346, 900913, always_xy=True)
#| (2770073.253734061, 7643086.849722873)

trans(6254761, 554539, 3346, 3857)  # PROJ swaps axes
#| (2770073.253734061, 7643086.849722873)

trans(554539, 6254761, 3346, 3857, always_xy=True)
#| (2770073.253734061, 7643086.849722873)


# WGS84 -> WGS84
trans(25.286698, 54.685699, 4326, 4326, always_xy=True)
#| (25.286698, 54.685699)

# WGS84 / Pseudo-Mercator -> WGS84
trans(2814902.175856, 7301104.229962, 3857, 4326, always_xy=True)
#| (25.286696478727414, 54.68569969658001)

# LKS94 -> WGS84
trans(582964.550544, 6061790.095239, 3346, 4326, always_xy=True)
#| (25.286697149277767, 54.68570085943802)

exit()


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
