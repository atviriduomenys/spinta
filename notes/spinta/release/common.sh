# Check outdated packages and upgrade them
xdg-open https://github.com/atviriduomenys/spinta/security/dependabot
poetry install --all-extras
poetry update


# Generate changes and readme html formats
# Prepare CHANGES.rst for release (change version from unreleased to actual, check if all changes added correctly)
poetry run rst2html CHANGES.rst var/changes.html
xdg-open var/changes.html
# alternative:
# poetry shell
# rst2html CHANGES.rst var/changes.html

poetry run rst2html README.rst var/readme.html
xdg-open var/readme.html


# Reset test database
# password is in the docker-compose.yml file
#If docker is not running, run `docker compose up`
export PGPASSWORD=admin123
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


# Configure spinta
# create a dir for all release-related files (e.g. 02dev2)
# BASEDIR should be set by respective release type (patch, rc, etc.)
# export BASEDIR=x (newly created release dir, better add full path)
mkdir -p "$BASEDIR"
#cat "$BASEDIR"/config.yml
cat > "$BASEDIR"/config.yml <<EOF
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


# Create manifest file
# this is a directory to which manifest data will be downloaded.
# If you don't have one, create an empty directory and cs into it.
# If you create a new directory, do this:
# git clone git@github.com:atviriduomenys/manifest.git
# git checkout get.data.gov.lt
# otherwise, do the following steps:
cd ~/dev/data/manifest
git status
git checkout get.data.gov.lt
git pull

# install the current spinta code or activate venv with it

find datasets -iname "*.csv" | xargs spinta check
cat get_data_gov_lt.in | xargs spinta copy -o "$BASEDIR"/manifest.csv
spinta check

spinta show \
    datasets/gov/rc/ar/adresai.csv \
    datasets/gov/rc/jar/formos_statusai.csv \
    datasets/gov/rc/jar/iregistruoti.csv

cat > "$BASEDIR"/sdsa.txt <<EOF
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

spinta check "$BASEDIR"/sdsa.txt

# Run migrations
spinta --tb native migrate --autocommit
spinta --tb native bootstrap

export PAGER="cat"
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'


# Run server in INTERNAL mode
test -n "$PID" && kill "$PID"
spinta run &>> "$BASEDIR"/spinta.log & PID=$!
#wait a bit for it to load
tail -50 "$BASEDIR"/spinta.log

http :8000
http :8000/version
http :8000/datasets/gov | jq -c '._data[]'

# Setup INTERNAL server data
SERVER=http://localhost:8000
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
  uapi:/:set_meta_fields
  uapi:/:getone
  uapi:/:getall
  uapi:/:search
  uapi:/:changes
  uapi:/:create
  uapi:/:upsert
  uapi:/:update
  uapi:/:patch
  uapi:/:delete
  uapi:/:wipe
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
echo "$AUTH"

http POST :8000/datasets/gov/rc/ar/adresai/Adresas "$AUTH" <<EOF
{
    "_id": "264ae0f9-53eb-496b-a07c-ce1b9cbe510c",
    "tipas": 2,
    "aob_kodas": 190557457,
    "aob_data_nuo": "2018-12-07"
}
EOF


# Run server in EXTERNAL mode
# Temporarily run with port 7000, so we can run another server with 8000 port
test -n "$EXTERNAL_PID" && kill "$EXTERNAL_PID"
spinta run --port 7000 --mode external "$BASEDIR"/sdsa.txt &>> "$BASEDIR"/spinta.log & EXTERNAL_PID=$!
tail -50 "$BASEDIR"/spinta.log

xdg-open http://localhost:7000/datasets/gov/rc/jar/formos_statusai/Forma
xdg-open http://localhost:7000/datasets/gov/rc/jar/formos_statusai/Statusas
xdg-open http://localhost:7000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
# Expected an error, since `Adresas` is running in internal mode
# Need to sync `Adresas` data from another server

# Sync data from INTERNAL server
spinta keymap sync "$BASEDIR"/sdsa.txt -i test@localhost

xdg-open http://localhost:7000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo
# No more errors, `adresas._id` == `264ae0f9-53eb-496b-a07c-ce1b9cbe510c`

# Turn off external server
test -n "$EXTERNAL_PID" && kill "$EXTERNAL_PID"


# Run smoke tests
test -f "$BASEDIR"/keymap.db && rm "$BASEDIR"/keymap.db
test -f "$BASEDIR"/push/localhost.db && rm "$BASEDIR"/push/localhost.db

# configure credentials.cfg according to this: https://spinta.readthedocs.io/en/latest/manual/access.html#client-credentials
# add ~/.config/spinta/credentials.cfg with this content:
#[test@localhost]
#server = http://localhost:8000
#client = client
#secret = secret
#scopes =
#  uapi:/:set_meta_fields
#  uapi:/:getone
#  uapi:/:getall
#  uapi:/:search
#  uapi:/:changes
#  uapi:/:create
#  uapi:/:update
#  uapi:/:patch
#  uapi:/:delete
#  uapi:/:wipe


spinta push "$BASEDIR"/sdsa.txt -o test@localhost

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


# change the version number in pyproject.toml

# Publish project to PyPI
# If you don't have token, see here: https://pypi.org/help/#apitoken and here: https://www.digitalocean.com/community/tutorials/how-to-publish-python-packages-to-pypi-using-poetry-on-ubuntu-22-04

poetry build
poetry publish
xdg-open https://pypi.org/project/spinta/
