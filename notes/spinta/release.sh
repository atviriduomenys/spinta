cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
export CURRENT_VERSION=0.1.62
export NEW_VERSION=0.1.63
export FUTURE_VERSION=0.1.64

# Create new branch to merge it using pull request
export BRANCH_NAME=prepare_$NEW_VERSION_version
git branch $BRANCH_NAME
git checkout $BRANCH_NAME
git status

# Check outdated packages
xdg-open https://github.com/atviriduomenys/spinta/security/dependabot

# Upgrade packages
poetry install
poetry update
#| Updating certifi (2024.2.2 -> 2024.6.2)
#| Updating idna (3.6 -> 3.7)
#| Updating urllib3 (2.2.1 -> 2.2.2)
#| Updating babel (2.14.0 -> 2.15.0)
#| Updating jinja2 (3.1.3 -> 3.1.4)
#| Updating packaging (23.2 -> 24.1)
#| Updating parso (0.8.3 -> 0.8.4)
#| Updating pycparser (2.21 -> 2.22)
#| Updating pygments (2.17.2 -> 2.18.0)
#| Updating requests (2.31.0 -> 2.32.3)
#| Updating traitlets (5.14.1 -> 5.14.3)
#| Updating exceptiongroup (1.2.0 -> 1.2.1)
#| Updating matplotlib-inline (0.1.6 -> 0.1.7)
#| Updating pluggy (1.4.0 -> 1.5.0)
#| Updating prompt-toolkit (3.0.43 -> 3.0.47)
#| Updating python-dateutil (2.8.2 -> 2.9.0.post0)
#| Updating tornado (6.4 -> 6.4.1)
#| Updating typing-extensions (4.10.0 -> 4.12.2)
#| Updating zipp (3.17.0 -> 3.19.2)
#| Updating anyio (4.3.0 -> 4.4.0)
#| Updating coverage (7.4.3 -> 7.5.4)
#| Updating cryptography (42.0.5 -> 42.0.8)
#| Updating fsspec (2024.2.0 -> 2024.6.0)
#| Updating httpcore (1.0.4 -> 1.0.5)
#| Updating importlib-metadata (7.0.1 -> 7.2.1)
#| Updating jsonpointer (2.4 -> 3.0.0)
#| Updating livereload (2.6.3 -> 2.7.0)
#| Updating mako (1.3.2 -> 1.3.5)
#| Updating mypy (1.8.0 -> 1.10.1)
#| Updating psutil (5.9.8 -> 6.0.0)
#| Updating pyparsing (3.1.1 -> 3.1.2)
#| Updating setuptools (69.1.1 -> 70.1.1)
#| Updating sqlalchemy (1.4.51 -> 1.4.52)
#| Updating aiofiles (23.2.1 -> 24.1.0)
#| Updating aiohttp (3.9.3 -> 3.9.5)
#| Updating geoalchemy2 (0.14.4 -> 0.15.1)
#| Updating gunicorn (21.2.0 -> 22.0.0)
#| Updating lxml (5.1.0 -> 5.2.2)
#| Updating msgpack (1.0.7 -> 1.0.8)
#| Updating openpyxl (3.1.2 -> 3.1.4)
#| Updating phonenumbers (8.13.31 -> 8.13.39)
#| Updating pymongo (4.6.2 -> 4.7.3)
#| Updating pytest-asyncio (0.23.5 -> 0.23.7)
#| Updating pytest-cov (4.1.0 -> 5.0.0)
#| Updating pytest-mock (3.12.0 -> 3.14.0)
#| Updating responses (0.25.0 -> 0.25.3)
#| Updating setuptools-scm (8.0.4 -> 8.1.0)
#| Updating shapely (2.0.3 -> 2.0.4)
#| Updating sqlalchemy-utils (0.41.1 -> 0.41.2)
#| Updating sqlparse (0.4.4 -> 0.5.0)
#| Updating starlette (0.37.1 -> 0.37.2)
#| Updating tqdm (4.66.2 -> 4.66.4)
#| Updating typer (0.9.0 -> 0.12.3)
#| Updating ujson (5.9.0 -> 5.10.0)
#| Updating uvicorn (0.27.1 -> 0.30.1)

head CHANGES.rst

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master
# Update CHANGES.rst

docker compose ps
docker compose up -d
unset SPINTA_CONFIG
unset SPINTA_CONFIG_PATH
test -n "$PID" && kill $PID

psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_tests'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_tests'
psql -h localhost -p 54321 -U admin spinta_tests <<EOF
BEGIN TRANSACTION;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF

poetry run pytest -vvx --tb=short tests
#| 2003 passed, 42 skipped, 2524 warnings in 338.17s (0:05:38)

# Fix warnings:
# pkg_resource deprecation
# shapely geometry type deprecation
# sqlalchemy LegacyRow.items() deprecation
# TestClientResponse being triggered as Test container

poetry run pytest -vvx --tb=short tests
#| 2003 passed, 42 skipped, 347 warnings in 307.40s (0:05:07)

poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

poetry run rst2html.py README.rst var/readme.html
xdg-open var/readme.html

# Check if new Spinta version works with manifest files
poetry shell

# Configure Spinta server instance
INSTANCE=releases/$NEW_VERSION
BASEDIR=$PWD/var/instances/$INSTANCE
mkdir -p $BASEDIR
cat $BASEDIR/config.yml
cat > $BASEDIR/config.yml <<EOF
env: production
data_path: $BASEDIR
default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$BASEDIR/keymap.db

backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@localhost:54321/spinta

  external:
    type: sql
    dsn: postgresql://admin:admin123@localhost:54321/spinta_external

manifest: default
manifests:
  default:
    type: csv
    path: $BASEDIR/manifest.csv
    backend: default
    keymap: default
    mode: internal

accesslog:
  type: file
  file: $BASEDIR/accesslog.json
EOF
export SPINTA_CONFIG=$BASEDIR/config.yml


# Reset EXTERNAL (source) database
psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_external'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_external'
psql -h localhost -p 54321 -U admin spinta_external <<EOF
CREATE TABLE formos (
    id integer primary key,
    kodas integer,
    pavadinimas text,
    pav_ilgas text,
    name varchar(255),
    tipas varchar(255),
    type varchar(255)
);
INSERT INTO formos
    (id, kodas, pavadinimas, pav_ilgas, name, tipas, type)
    VALUES 
    (1, 950, 'Biudžetinė įstaiga', 'Biudžetinė įstaiga', 'Budget Institution', 'Viešasis', 'Public');

CREATE TABLE statusai (
    kodas integer primary key,
    pavadinimas text,
    name varchar(255)
);
INSERT INTO statusai
    (kodas, pavadinimas, name)
    VALUES 
    (0, 'Biudžetinė įstaiga', 'No legal proceedings');

CREATE TABLE iregistruoti (
    ja_kodas integer primary key,
    ja_pavadinimas text,
    pilnas_adresas text,
    reg_data varchar(10),
    stat_data varchar(10),
    forma_id integer,
    statusas_id integer,
    adreso_kodas integer
);
INSERT INTO iregistruoti
    (ja_kodas, ja_pavadinimas, pilnas_adresas, reg_data, stat_data, forma_id, statusas_id, adreso_kodas)
    VALUES 
    (188772433, 'Informacinės visuomenės plėtros komitetas', 'Vilnius, Konstitucijos pr. 15-89', '2001-08-01', '2001-08-01', 1, 0, 190557457);
EOF
psql -h localhost -p 54321 -U admin spinta_external -c '\dt public.*'
psql -h localhost -p 54321 -U admin spinta_external -c 'select * from formos;'
psql -h localhost -p 54321 -U admin spinta_external -c 'select * from statusai;'
psql -h localhost -p 54321 -U admin spinta_external -c 'select * from iregistruoti;'


# Create manifest file
cd ~/dev/data/manifest
git status
git checkout get.data.gov.lt
git pull
find datasets -iname "*.csv" | xargs spinta check
cat get_data_gov_lt.in | xargs spinta copy -o $BASEDIR/manifest.csv
spinta check

spinta show \
    datasets/gov/rc/ar/adresai.csv \
    datasets/gov/rc/jar/formos_statusai.csv \
    datasets/gov/rc/jar/iregistruoti.csv

cat > $BASEDIR/sdsa.txt <<EOF
d | r | b | m | property            | type    | ref                                           | source         | prepare | level | access
datasets/gov/rc/ar/adresai          |         |                                               |                |         |       |
  |   |   | Adresas                 |         | aob_kodas                                     |                |         | 4     |
  |   |   |   | tipas               | integer |                                               |                |         | 4     | open
  |   |   |   | aob_kodas           | integer |                                               |                |         | 4     | open
  |   |   |   | aob_data_nuo        | date    | D                                             |                |         | 4     | open
  |   |   |   | aob_data_iki        | date    | D                                             |                |         | 4     | open
                                    |         |                                               |                |         |       |
datasets/gov/rc/jar/formos_statusai |         |                                               |                |         |       |
  | db                              | sql     | external                                      |                |         |       |
  |   |   | Forma                   |         | id                                            | formos         |         | 4     |
  |   |   |   | id                  | integer |                                               | id             |         | 4     | protected
  |   |   |   | kodas               | integer |                                               | kodas          |         | 4     | open
  |   |   |   | pavadinimas         | string  |                                               | pavadinimas    |         | 3     | open
  |   |   |   | pav_ilgas           | string  |                                               | pav_ilgas      |         | 3     | open
  |   |   |   | name                | string  |                                               | name           |         | 3     | open
  |   |   |   | tipas               | string  |                                               | tipas          |         | 4     | open
                                    | enum    |                                               | Privatus       |         |       |
                                    |         |                                               | Viešasis       |         |       |
  |   |   |   | type                | string  |                                               | type           |         | 4     | open
                                    | enum    |                                               | Private        |         |       |
                                    |         |                                               | Public         |         |       |
                                    |         |                                               |                |         |       |
  |   |   | Statusas                |         | kodas                                         | statusai       |         | 4     |
  |   |   |   | kodas               | integer |                                               | kodas          |         | 4     | open
  |   |   |   | pavadinimas         | string  |                                               | pavadinimas    |         | 3     | open
  |   |   |   | name                | string  |                                               | name           |         | 3     | open
                                    |         |                                               |                |         |       |
datasets/gov/rc/jar/iregistruoti    |         |                                               |                |         |       |
  | db                              | sql     | external                                      |                |         |       |
  |   |   | JuridinisAsmuo          |         | ja_kodas                                      | iregistruoti   |         | 4     |
  |   |   |   | ja_kodas            | integer |                                               | ja_kodas       |         | 4     | open
  |   |   |   | ja_pavadinimas      | string  |                                               | ja_pavadinimas |         | 4     | open
  |   |   |   | pilnas_adresas      | string  |                                               | pilnas_adresas |         | 1     | open
  |   |   |   | adresas             | ref     | /datasets/gov/rc/ar/adresai/Adresas           | adreso_kodas   |         | 4     | open
  |   |   |   | reg_data            | date    | D                                             | reg_data       |         | 4     | open
  |   |   |   | forma               | ref     | /datasets/gov/rc/jar/formos_statusai/Forma    | forma_id       |         | 4     | open
  |   |   |   | statusas            | ref     | /datasets/gov/rc/jar/formos_statusai/Statusas | statusas_id    |         | 4     | open
EOF
spinta check $BASEDIR/sdsa.txt

test -n "$PID" && kill $PID
spinta run --mode external $BASEDIR/sdsa.txt &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

xdg-open http://localhost:8000/datasets/gov/rc/jar/formos_statusai/Forma
xdg-open http://localhost:8000/datasets/gov/rc/jar/formos_statusai/Statusas
xdg-open http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo


# Reset INTERNAL database
psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta'
psql -h localhost -p 54321 -U admin spinta <<EOF
BEGIN TRANSACTION;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF

# Run migrations
spinta --tb native migrate --autocommit
spinta --tb native bootstrap

export PAGER="cat"
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
#| (2714 rows)

# Run server
test -n "$PID" && kill $PID
spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Check if server works
http :8000
http :8000/version
http :8000/datasets/gov | jq -c '._data[]'

# Run some smoke tests

# Don't forget to add client to server and credentials;
# - notes/spinta/server.sh
# - notes/spinta/push.sh

SERVER=:8000
CLIENT=test
SECRET=secret
SCOPES="
    spinta_set_meta_fields
    spinta_getone
    spinta_getall
    spinta_search
    spinta_changes
    spinta_insert
    spinta_upsert
    spinta_update
    spinta_patch
    spinta_delete
    spinta_wipe
"
TOKEN=$(
    http \
        -a $CLIENT:$SECRET \
        -f $SERVER/auth/token \
        grant_type=client_credentials \
        scope="$SCOPES" \
    | jq -r .access_token
)
AUTH="Authorization: Bearer $TOKEN"
echo $AUTH

http POST :8000/datasets/gov/rc/ar/adresai/Adresas "$AUTH" <<EOF
{
    "_id": "264ae0f9-53eb-496b-a07c-ce1b9cbe510c",
    "tipas": 2,
    "aob_kodas": 190557457,
    "aob_data_nuo": "2018-12-07"
}
EOF

test -f $BASEDIR/keymap.db && rm $BASEDIR/keymap.db
test -f $BASEDIR/push/localhost.db && rm $BASEDIR/push/localhost.db

spinta push $BASEDIR/sdsa.txt -o test@localhost

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(json)"
http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(csv)"
http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(ascii)"
http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(rdf)"
http GET ":8000/datasets/gov/rc/jar/:all?format(rdf)"

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo/:changes"

xdg-open http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
xdg-open "http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)"
xdg-open http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/264ae0f9-53eb-496b-a07c-ce1b9cbe510c
xdg-open http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/:changes
xdg-open http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/264ae0f9-53eb-496b-a07c-ce1b9cbe510c/:changes

test -n "$PID" && kill $PID
unset SPINTA_CONFIG
exit
docker compose down

ed pyproject.toml <<EOF
/^version = /c
version = "$NEW_VERSION"
.
wq
EOF
ed CHANGES.rst <<EOF
/unreleased/c
$NEW_VERSION ($(date +%Y-%m-%d))
.
wq
EOF
git diff

poetry build
poetry publish
xdg-open https://pypi.org/project/spinta/
git commit -a -m "Releasing version $NEW_VERSION"
git push origin $BRANCH_NAME

# Create pull request in github and merge to master

git tag -a $NEW_VERSION -m "Releasing version $NEW_VERSION"
git push origin $NEW_VERSION

ed pyproject.toml <<EOF
/^version = /c
version = "$FUTURE_VERSION.dev0"
.
wq
EOF
ed CHANGES.rst <<EOF
/^###/a

$FUTURE_VERSION (unreleased)
===================

.
wq
EOF
head CHANGES.rst
git diff
git commit -a -m "Prepare for the next $FUTURE_VERSION release"
git push origin master
git log -n3
