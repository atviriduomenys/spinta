# 2023-04-14 09:39

# notes/sqlite.sh           Configure SQLite
# notes/docker.sh           Start docker compose
# notes/postgres.sh         Reset database

INSTANCE=cli/inspect
DATASET=$INSTANCE
# notes/spinta/server.sh    Configure server


rm $BASEDIR/db.sqlite
sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS countries (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    continent_id INTEGER
);
CREATE TABLE IF NOT EXISTS cities (
    id           INTEGER PRIMARY KEY,
    name         TEXT,
    country_id   INTEGER,
    FOREIGN KEY  (country_id) REFERENCES countries (id)
);
CREATE VIEW cities_view AS
SELECT id, name FROM cities;
EOF
sqlite3 $BASEDIR/db.sqlite ".schema"

unset SPINTA_CONFIG
test -f $BASEDIR/config.yml && rm $BASEDIR/config.yml

poetry run spinta config manifest
#| Origin  Name                       Value   
#| ------  -------------------------  --------
#| spinta  manifests.default.type     memory  
#| spinta  manifests.default.backend          
#| spinta  manifests.default.mode     internal
#| spinta  manifests.default.keymap           
#| spinta  manifest                   default 

poetry run spinta --tb=native inspect -r sql sqlite:///$BASEDIR/db.sqlite -o $BASEDIR/manifest.csv
#| Loading MemoryManifest manifest default
#| Loading InlineManifest manifest default

poetry run spinta show $BASEDIR/manifest.csv
#| id | d | r | b | m | property     | type    | ref       | source                                        | prepare | level | access | uri | title | description
#|    | dbsqlite                     |         |           |                                               |         |       |        |     |       |
#|    |   | resource1                | sql     |           | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
#|    |                              |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | Cities           |         | id        | cities                                        |         |       |        |     |       |
#|    |   |   |   |   | country_id   | ref     | Countries | country_id                                    |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
#|    |                              |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | Countries        |         | id        | countries                                     |         |       |        |     |       |
#|    |   |   |   |   | continent_id | integer |           | continent_id                                  |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
#|    | dbsqlite/views               |         |           |                                               |         |       |        |     |       |
#|    |   | resource1                | sql     |           | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
#|    |                              |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | CitiesView       |         |           | cities_view                                   |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
id | d | r | b | m | property     | type    | ref       | source                                        | prepare | level | access | uri | title | description
   | datasets/examplle            |         |           |                                               |         |       |        |     |       |
   |   | db                       | sql     |           | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
   |                              |         |           |                                               |         |       |        |     |       |
   |   |   |   | City             |         | id        | cities                                        |         |       |        |     |       |
   |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
   |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
   |   |   |   |   | country      | ref     | Country   | country_id                                    |         |       |        |     |       |
   |                              |         |           |                                               |         |       |        |     |       |
   |   |   |   | Country          |         | id        | countries                                     |         |       |        |     |       |
   |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
   |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
   |   |   |   |   | continent_id | integer |           | continent_id                                  |         |       |        |     |       |
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv


sqlite3 $BASEDIR/db.sqlite <<'EOF'
CREATE TABLE IF NOT EXISTS continents (
    id           INTEGER PRIMARY KEY,
    name         TEXT
);
EOF
sqlite3 $BASEDIR/db.sqlite ".schema"

poetry run spinta inspect $BASEDIR/manifest.csv -o $BASEDIR/manifest.csv
#| Loading InlineManifest manifest default
#| Loading InlineManifest manifest default

poetry run spinta show $BASEDIR/manifest.csv
#| id | d | r | b | m | property     | type    | ref     | source                                        | prepare | level | access | uri | title | description
#|    | datasets/examplle            |         |         |                                               |         |       |        |     |       |
#|    |   | db                       | sql     |         | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
#|    |                              |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | City             |         | id      | cities                                        |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |         | name                                          |         |       |        |     |       |
#|    |   |   |   |   | country      | ref     | Country | country_id                                    |         |       |        |     |       |
#|    |                              |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | Country          |         | id      | countries                                     |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |         | name                                          |         |       |        |     |       |
#|    |   |   |   |   | continent_id | integer |         | continent_id                                  |         |       |        |     |       |
#|    |                              |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | Continents       |         | id      | continents                                    |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |         | name                                          |         |       |        |     |       |
#|    |                              |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | CitiesView       |         |         | cities_view                                   |         |       |        |     |       |
#|    |   |   |   |   | id           | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name         | string  |         | name                                          |         |       |        |     |       |


cat > $BASEDIR/manifest.txt <<EOF
id | d | r | b | m | property     | type    | ref       | source                                        | prepare | level | access | uri | title | description
   | datasets/examplle            |         |           |                                               |         |       |        |     |       |
   |   | db                       | sql     |           | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
   |                              |         |           |                                               |         |       |        |     |       |
   |   |   |   | City             |         | id        | cities                                        |         |       |        |     |       |
   |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
   |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
   |   |   |   |   | country      | ref     | Country   | country_id                                    |         |       |        |     |       |
   |                              |         |           |                                               |         |       |        |     |       |
   |   |   |   | Country          |         | id        | countries                                     |         |       |        |     |       |
   |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
   |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
   |   |   |   |   | continent    | ref     | Continent | continent_id                                  |         |       |        |     |       |
   |                              |         |           |                                               |         |       |        |     |       |
   |   |   |   | Continent        |         | id        | continents                                    |         |       |        |     |       |
   |   |   |   |   | id           | integer |           | id                                            |         |       |        |     |       |
   |   |   |   |   | name         | string  |           | name                                          |         |       |        |     |       |
EOF
poetry run spinta copy $BASEDIR/manifest.txt -o $BASEDIR/manifest.csv
cat $BASEDIR/manifest.csv
poetry run spinta show $BASEDIR/manifest.csv


sqlite3 $BASEDIR/db.sqlite <<'EOF'
ALTER TABLE "countries" ADD COLUMN "population" INTEGER;
EOF
sqlite3 $BASEDIR/db.sqlite ".schema"

poetry run spinta inspect $BASEDIR/manifest.csv -o $BASEDIR/manifest.csv
#| Loading InlineManifest manifest default
#| Loading InlineManifest manifest default

poetry run spinta show $BASEDIR/manifest.csv
#| id | d | r | b | m | property   | type    | ref       | source                                        | prepare | level | access | uri | title | description
#|    | datasets/examplle          |         |           |                                               |         |       |        |     |       |
#|    |   | db                     | sql     |           | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
#|    |                            |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | City           |         | id        | cities                                        |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |           | name                                          |         |       |        |     |       |
#|    |   |   |   |   | country    | ref     | Country   | country_id                                    |         |       |        |     |       |
#|    |                            |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | Country        |         | id        | countries                                     |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |           | name                                          |         |       |        |     |       |
#|    |   |   |   |   | continent  | ref     | Continent | continent_id                                  |         |       |        |     |       |
#|    |   |   |   |   | population | integer |           | population                                    |         |       |        |     |       |
#|    |                            |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | Continent      |         | id        | continents                                    |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |           | name                                          |         |       |        |     |       |
#|    |                            |         |           |                                               |         |       |        |     |       |
#|    |   |   |   | CitiesView     |         |           | cities_view                                   |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |           | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |           | name                                          |         |       |        |     |       |


sqlite3 $BASEDIR/db.sqlite <<'EOF'
DROP TABLE "continents";
EOF
sqlite3 $BASEDIR/db.sqlite ".schema"

poetry run spinta inspect $BASEDIR/manifest.csv -o $BASEDIR/manifest.csv
#| Loading InlineManifest manifest default
#| Loading InlineManifest manifest default

poetry run spinta show $BASEDIR/manifest.csv
#| id | d | r | b | m | property   | type    | ref     | source                                        | prepare | level | access | uri | title | description
#|    | datasets/examplle          |         |         |                                               |         |       |        |     |       |
#|    |   | db                     | sql     |         | sqlite:///var/instances/cli/inspect/db.sqlite |         |       |        |     |       |
#|    |                            |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | City           |         | id      | cities                                        |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |         | name                                          |         |       |        |     |       |
#|    |   |   |   |   | country    | ref     | Country | country_id                                    |         |       |        |     |       |
#|    |                            |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | Country        |         | id      | countries                                     |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |         | id                                            |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |         | name                                          |         |       |        |     |       |
#|    |   |   |   |   | continent  | integer |         | continent_id                                  |         |       |        |     |       |
#|    |   |   |   |   | population | integer |         | population                                    |         |       |        |     |       |
#|    |                            |         |         |                                               |         |       |        |     |       |
#|    |   |   |   | Continent      |         | id      |                                               |         |       |        |     |       |
#|    |   |   |   |   | id         | integer |         |                                               |         |       |        |     |       |
#|    |   |   |   |   | name       | string  |         |                                               |         |       |        |     |       |
