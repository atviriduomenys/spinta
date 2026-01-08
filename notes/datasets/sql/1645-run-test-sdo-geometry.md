# OracleXE. SDO_GEOMETRY. Testing

## Install Oracle Instant Client for Oracle DB backend of Spinta

```zsh
# Install Oracle Instant Client libraries
sudo apt install libaio1
sudo apt intall libaio
# create folder for Oracle Instant Client
export ORACLE_HOME=$HOME/oracle
export ORACLE_LIB=$HOME/oracle/instantclient_23_26
mkdir -p $ORACLE_HOME
# download Oracle Instant Client
wget https://download.oracle.com/otn_software/linux/instantclient/2326000/instantclient-basiclite-linux.x64-23.26.0.0.0.zip -P $ORACLE_HOME
unzip $ORACLE_HOME/instantclient-basiclite-linux.x64-23.26.0.0.0.zip -d $ORACLE_HOME
ls -l $ORACLE_LIB
#
export LD_LIBRARY_PATH=$HOME/oracle/instantclient_23_26
# or
# export LD_LIBRARY_PATH=$ORACLE_LIB:$LD_LIBRARY_PATH
sudo ldconfig
#
pip install cx_Oracle
```

## Create OracleXE container

```zsh
docker run -d -p 1521:1521 --name oracle -e ORACLE_PASSWORD=admin123 -v oracle-volume:/opt/oracle/oradata gvenzl/oracle-xe
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

```zsh
docker exec -it oracle sqlplus spinta/admin123
```

Use [Oracle SQL SDO_GEOMETRY](https://docs.oracle.com/en/database/oracle/oracle-database/26/spatl/simple-example-inserting-indexing-and-querying-spatial-data.html) to create table with `SDO_GEOMETRY` column.

```zsh

INSERT INTO cola_markets VALUES(
  100,
  'Vilnius',
    SDO_GEOMETRY(
        2001, -- GTYPE: 2=2D, 0=Point, 1=Ordinates
        NULL, -- SRID (Spatial Reference ID) - often NULL or specific ID
        SDO_POINT_TYPE( 54.685691, 25.286688, NULL ), -- SDO_POINT(X, Y, Z) Vilnius Bok
        NULL, -- SDO_ELEM_INFO (NULL for simple points)
        NULL  -- SDO_ORDINATES (NULL for simple points)
    )
);

COMMIT;
EXIT;
```

## Run spinta inspect

```zsh
poetry run spinta inspect -r sql oracle+cx_oracle://spinta:admin123@localhost:1521/xe -f "connect(schema: spinta)" -o dist/manifests/spinta.csv
code dist/manifests/spinta.csv
soffice dist/manifests/spinta.csv
```

## Run spinta run

```zsh
poetry run spinta run --mode external dist/manifests/spinta.csv
```

## Cleanup

### DB table and user

```sql
DROP TABLE cola_markets;
DROP INDEX cola_spatial_idx;
DROP USER spinta;
DELETE FROM user_sdo_geom_metadata WHERE TABLE_NAME = 'COLA_MARKETS';
SELECT * FROM user_sdo_geom_metadata;
```

### Docker conontainer

```zsh
docker stop oracle
docker rm oracle
docker volume rm oracle-volume
docker rmi gvenzl/oracle-xe
rm -rf $HOME/oracle
```
