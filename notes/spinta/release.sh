cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
export CURRENT_VERSION=0.1.56
export NEW_VERSION=0.1.57
export FUTURE_VERSION=0.1.58

head CHANGES.rst

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master
# Update CHANGES.rst

docker-compose ps
docker-compose up -d
unset SPINTA_CONFIG
test -n "$PID" && kill $PID

psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_tests'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_tests'
psql -h localhost -p 54321 -U admin spinta <<EOF
BEGIN TRANSACTION;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF

poetry run pytest -vvx --tb=short tests
#| 1486 passed, 39 skipped, 203 warnings in 263.34s (0:04:23)

poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

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

tree ~/.config/spinta/
poetry run spinta genkeys

# Add default client
cat ~/.config/spinta/clients/default.yml
poetry run spinta client add -n default --add-secret --scope - <<EOF
spinta_getone
spinta_getall
spinta_search
spinta_changes
EOF

# Create manifest file
cd ~/dev/data/manifest
git status
git checkout get.data.gov.lt
git pull
find datasets -iname "*.csv" | xargs spinta check
cat get_data_gov_lt.in | xargs spinta copy -o $BASEDIR/manifest.csv
spinta check

# Reset database
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
spinta bootstrap

export PAGER="cat"
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'

# Run server
test -n "$PID" && kill $PID
spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Check if server works
http :8000
http :8000/version

# Run some smoke tests

SERVER=:8000
CLIENT=test
SECRET=secret
SCOPES=(
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
)
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

http POST :8000/datasets/gov/rc/ar/adresai/Adresas $AUTH <<EOF
{
    "_id": "264ae0f9-53eb-496b-a07c-ce1b9cbe510c",
    "tipas": 2,
    "aob_kodas": 190557457,
    "aob_data_nuo": "2018-12-07"
}
EOF
http POST :8000/datasets/gov/rc/jar/formos_statusai/Forma $AUTH <<EOF
{
    "_id": "c7fda07b-1689-42d3-8412-24d375f01bcb",
    "kodas": 950,
    "pavadinimas": "Biudžetinė įstaiga",
    "pav_ilgas": "Biudžetinė įstaiga",
    "name": "Budget Institution",
    "tipas": "Viešasis",
    "type": "Public"
}
EOF
http POST :8000/datasets/gov/rc/jar/formos_statusai/Statusas $AUTH <<EOF
{
    "_id": "5ef6b364-a5ff-47fb-8600-ff859214ef85",
    "kodas": 0,
    "pavadinimas": "Teisinis statusas neįregistruotas",
    "name": "No legal proceedings"
}
EOF
http POST :8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo $AUTH <<EOF
{
    "_id": "cf8e5cfd-32b3-4f24-a89b-e7c94bbd1c36",
    "ja_kodas": 188772433,
    "ja_pavadinimas": "Informacinės visuomenės plėtros komitetas",
    "pilnas_adresas": "Vilnius, Konstitucijos pr. 15-89",
    "reg_data": "2001-08-01",
    "stat_data": "2001-08-01",
    "forma": {"_id": "c7fda07b-1689-42d3-8412-24d375f01bcb"},
    "statusas": {"_id": "5ef6b364-a5ff-47fb-8600-ff859214ef85"},
    "adresas": {"_id": "264ae0f9-53eb-496b-a07c-ce1b9cbe510c"}
}
EOF

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(json)"

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(csv)"

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(ascii)"

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(rdf)"

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo/:changes"

xdg-open http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo

test -n "$PID" && kill $PID
unset SPINTA_CONFIG
exit
docker-compose down

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
git push origin master
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
