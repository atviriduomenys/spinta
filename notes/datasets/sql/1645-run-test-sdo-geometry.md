# OracleXE. SDO_GEOMETRY. Testing

## Create OracleXE container

```zsh
# start OracleXE and wait for it to be ready (about 30 seconds)
docker run -d -p 1521:1521 --name oracle -e ORACLE_PASSWORD=admin123 -v oracle-volume:/opt/oracle/oradata gvenzl/oracle-xe;  docker logs -f oracle
# Ctrl+C
docker ps
```

## Create user `spinta` and tables

```zsh
docker exec -it oracle sqlplus system/admin123 as sysdba
```

```sql
CREATE USER spinta IDENTIFIED BY admin123;
GRANT RESOURCE, CREATE SESSION TO spinta;
ALTER USER spinta QUOTA UNLIMITED ON USERS;
EXIT;
```

## Connect to OracleXE

```zsh
docker exec -it oracle sqlplus spinta/admin123
```

## Create table with `SDO_GEOMETRY` column

Use [Oracle SQL SDO_GEOMETRY](https://docs.oracle.com/en/database/oracle/oracle-database/26/spatl/simple-example-inserting-indexing-and-querying-spatial-data.html) to create table with `SDO_GEOMETRY` column.

```sql
CREATE TABLE cola_markets (
  mkt_id NUMBER PRIMARY KEY,
  name VARCHAR2(32),
  shape SDO_GEOMETRY);
COMMIT;

INSERT INTO cola_markets VALUES(
  1,
  'cola_a',
  SDO_GEOMETRY(
    SDO_POLYGON2D,  -- two-dimensional polygon
    NULL,
    NULL,
    SDO_ELEM_INFO_ARRAY(1,1003,3), -- one rectangle (1003 = exterior)
    SDO_ORDINATE_ARRAY(1,1, 5,7) -- only 2 points needed to
          -- define rectangle (lower left and upper right) with
          -- Cartesian-coordinate data
  )
);

INSERT INTO cola_markets VALUES(
  2,
  'cola_b',
  SDO_GEOMETRY(
    SDO_POLYGON2D,  -- two-dimensional polygon
    NULL,
    NULL,
    SDO_ELEM_INFO_ARRAY(1,1003,1), -- one polygon (exterior polygon ring)
    SDO_ORDINATE_ARRAY(5,1, 8,1, 8,6, 5,7, 5,1)
  )
);

INSERT INTO user_sdo_geom_metadata
    (TABLE_NAME,
     COLUMN_NAME,
     DIMINFO,
     SRID)
  VALUES (
  'cola_markets',
  'shape',
  SDO_DIM_ARRAY(   -- 20X20 grid
    SDO_DIM_ELEMENT('X', 0, 20, 0.005),
    SDO_DIM_ELEMENT('Y', 0, 20, 0.005)
     ),
  NULL   -- SRID
);
COMMIT;

CREATE INDEX cola_spatial_idx ON cola_markets(shape) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2;
COMMIT;

-- latitude, longitude of Cathedral Bell Tower Clock
INSERT INTO cola_markets VALUES(
  3,
  'Vilnius',
    SDO_GEOMETRY(
        2001, -- GTYPE: 2=2D, 0=Point, 1=Ordinates
        NULL, -- SRID (Spatial Reference ID) - often NULL or specific ID
        SDO_POINT_TYPE( 54.685691, 25.286688, NULL ), -- SDO_POINT(X, Y, Z) Vilnius Boksto laikrodis
        NULL, -- SDO_ELEM_INFO (NULL for simple points)
        NULL  -- SDO_ORDINATES (NULL for simple points)
    )
);
COMMIT;
EXIT;
```

## Install Oracle Instant Client for Oracle DB backend of Spinta

```zsh
# Install Oracle Instant Client libraries
sudo apt install libaio1
# create folder for Oracle Instant Client
export ORACLE_HOME=$HOME/oracle
mkdir -p $ORACLE_HOME
# download Oracle Instant Client
wget https://download.oracle.com/otn_software/linux/instantclient/2326000/instantclient-basiclite-linux.x64-23.26.0.0.0.zip -P $ORACLE_HOME
sudo apt install unzip
unzip $ORACLE_HOME/instantclient-basiclite-linux.x64-23.26.0.0.0.zip -d $ORACLE_HOME
#
ls -al $HOME/oracle/instantclient_23_26
# this is needed for cx_Oracle to find Oracle Instant Client libraries
# do this every time you open a new terminal!!!
export LD_LIBRARY_PATH=$HOME/oracle/instantclient_23_26
sudo ldconfig

# Install cx_Oracle
sudo apt install python3-pip
poetry run pip install cx_Oracle
# Test cx_Oracle
poetry run python3 -c "import cx_Oracle; print(cx_Oracle.clientversion())"
# Test cx_Oracle connection
poetry run python3 -c "import cx_Oracle; conn = cx_Oracle.connect(\"spinta/admin123@localhost:1521/xe\"); print(conn.version); conn.close()"
# 21.3.0.0.0
```

## Clone GitHub repo

```zsh
sudo apt install git curl
git clone git@github.com:atviriduomenys/spinta.git
cd spinta
# install poetry
curl -sSL https://install.python-poetry.org | python3 -
export PATH="/home/spinta/.local/bin:$PATH"
poetry --version
poetry install --all-extras -vvv
```

## Run spinta inspect

```zsh
poetry run spinta inspect -r sql oracle+cx_oracle://spinta:admin123@loalhost:1521/xe -f "connect(schema: spinta)" -o dist/manifests/spinta.csv
# native trace log
poetry run spinta --tb native inspect -r sql oracle+cx_oracle://spinta:admin123@localhost:1521/xe -f "connect(schema: spinta)" -o dist/manifests/spinta.csv
```

## Run spinta run

```zsh
# Update dist/manifests/spinta.csv:
# 1. write "open" to column "access"
# 2. add password "admin123" to URL
soffice dist/manifests/spinta.csv
poetry run spinta run --mode external dist/manifests/spinta.csv
```

## Cleanup

### Remove table and user

```sql
DROP TABLE cola_markets;
DROP INDEX cola_spatial_idx;
DROP USER spinta;
DELETE FROM user_sdo_geom_metadata WHERE TABLE_NAME = 'COLA_MARKETS';
SELECT * FROM user_sdo_geom_metadata;
COMMIT;
```

### Remove docker conontainer

```zsh
docker stop oracle
docker rm oracle
docker volume rm oracle-volume
docker rmi gvenzl/oracle-xe
rm -rf $HOME/oracle
```
