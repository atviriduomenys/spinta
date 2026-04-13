# =============================================================================
# RELEASE SCRIPT FOR SPINTA STABLE VERSION
# Change version numbers below before running.
# =============================================================================

export MAJOR=0
export MINOR=2
export RELEASE_VERSION=$MAJOR.$MINOR
export CURRENT_PATCH=0  # patch version of the last release
export NEW_PATCH=0      # patch version being released now
export FUTURE_PATCH=1   # patch version to prepare after release

export CURRENT_VERSION=$RELEASE_VERSION.$CURRENT_PATCH
export NEW_VERSION=$RELEASE_VERSION.$NEW_PATCH
export FUTURE_VERSION=$RELEASE_VERSION.$FUTURE_PATCH

export RELEASE_BRANCH=prepare_${NEW_VERSION}_version

echo "=== Releasing spinta $NEW_VERSION (next: $FUTURE_VERSION) ==="

echo "--- Setting up release branch ---"
git status

# Switch to master and get the latest changes before branching
git checkout master
[ $? -eq 0 ] || { echo "ERROR: Failed to checkout master"; exit 1; }

git pull
[ $? -eq 0 ] || { echo "ERROR: Failed to pull latest changes"; exit 1; }

# Create and switch to the release branch
export RELEASE_BRANCH=release_${NEW_VERSION}_version

git branch $RELEASE_BRANCH
[ $? -eq 0 ] || echo "WARNING: Failed to create branch $RELEASE_BRANCH (it may already exist), continuing..."

git checkout $RELEASE_BRANCH
[ $? -eq 0 ] || { echo "ERROR: Failed to checkout $RELEASE_BRANCH"; exit 1; }

# Verify we are on the correct branch before proceeding
current_branch=$(git branch --show-current)
[ "$current_branch" = "$RELEASE_BRANCH" ] || { echo "ERROR: Expected branch $RELEASE_BRANCH, but on $current_branch"; exit 1; }

git status

echo "--- Installing and updating dependencies ---"
# Install all dependencies and update them to latest compatible versions
poetry install --all-extras
[ $? -eq 0 ] || { echo "ERROR: poetry install failed"; exit 1; }

poetry update
[ $? -eq 0 ] || { echo "ERROR: poetry update failed"; exit 1; }

echo "--- Upgrading documentation dependencies ---"
# Upgrade documentation dependencies
(cd docs && make upgrade)
[ $? -eq 0 ] || { echo "ERROR: docs upgrade failed"; exit 1; }

# Print the diff URL so you can review what changed since the last release
echo "https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master"

# Preview the top of CHANGES.rst to check it is up to date
head CHANGES.rst

# Pause to allow manually updating CHANGES.rst if needed
read -n1 -p "Press Enter to continue or Esc to stop..."
echo
[ "$REPLY" = $'\e' ] && { echo "Stopped by user"; exit 1; }

echo "--- Generating HTML previews of CHANGES.rst and README.rst ---"
# Generate HTML previews of the changelog and readme for visual inspection
poetry run rst2html CHANGES.rst var/changes.html
[ $? -eq 0 ] || { echo "ERROR: Failed to generate changes.html"; exit 1; }
xdg-open var/changes.html
# alternative:
# poetry shell
# rst2html CHANGES.rst var/changes.html

poetry run rst2html README.rst var/readme.html
[ $? -eq 0 ] || { echo "ERROR: Failed to generate readme.html"; exit 1; }
xdg-open var/readme.html

# Pause to review the generated HTML files before continuing
read -n1 -p "Please check the generated Changes and Readme files. Press Enter to continue or Esc to stop..."
echo
[ "$REPLY" = $'\e' ] && { echo "Stopped by user"; exit 1; }

echo "--- Starting Docker containers ---"
# Start the database container
docker compose up -d
[ $? -eq 0 ] || { echo "ERROR: docker compose up failed"; exit 1; }

echo "--- Resetting test database ---"
# Reset the test database — drop if exists, then recreate with required PostGIS extensions
export PGPASSWORD=admin123
psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_tests'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_tests'
[ $? -eq 0 ] || { echo "ERROR: Failed to create spinta_tests database"; exit 1; }

psql -h localhost -p 54321 -U admin spinta_tests <<EOF
BEGIN TRANSACTION;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF
[ $? -eq 0 ] || { echo "ERROR: Failed to setup spinta_tests extensions"; exit 1; }

echo "--- Setting up external database with fixture data ---"
# Reset the external (source) database with test fixture data used for smoke tests
psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta_external'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta_external'
[ $? -eq 0 ] || { echo "ERROR: Failed to create spinta_external database"; exit 1; }

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
[ $? -eq 0 ] || { echo "ERROR: Failed to setup spinta_external tables"; exit 1; }

echo "--- Verifying external database contents ---"
# Verify all expected tables and fixture data exist in spinta_external
tables=$(psql -h localhost -p 54321 -U admin spinta_external -c '\dt public.*')
echo "$tables" | grep -q "formos" || { echo "ERROR: Table formos not found"; exit 1; }
echo "$tables" | grep -q "statusai" || { echo "ERROR: Table statusai not found"; exit 1; }
echo "$tables" | grep -q "iregistruoti" || { echo "ERROR: Table iregistruoti not found"; exit 1; }

formos=$(psql -h localhost -p 54321 -U admin spinta_external -c 'select * from formos;')
echo "$formos" | grep -q "950" || { echo "ERROR: Expected kodas=950 in formos"; exit 1; }
echo "$formos" | grep -q "Budget Institution" || { echo "ERROR: Expected name='Budget Institution' in formos"; exit 1; }

statusai=$(psql -h localhost -p 54321 -U admin spinta_external -c 'select * from statusai;')
echo "$statusai" | grep -q "No legal proceedings" || { echo "ERROR: Expected name='No legal proceedings' in statusai"; exit 1; }

iregistruoti=$(psql -h localhost -p 54321 -U admin spinta_external -c 'select * from iregistruoti;')
echo "$iregistruoti" | grep -q "188772433" || { echo "ERROR: Expected ja_kodas=188772433 in iregistruoti"; exit 1; }
echo "$iregistruoti" | grep -q "Informacinės visuomenės plėtros komitetas" || { echo "ERROR: Expected ja_pavadinimas in iregistruoti"; exit 1; }

echo "--- Resetting internal database ---"
# Reset the internal (spinta) database — drop if exists, then recreate with PostGIS extensions
psql -h localhost -p 54321 -U admin postgres -c 'DROP DATABASE spinta'
psql -h localhost -p 54321 -U admin postgres -c 'CREATE DATABASE spinta'
[ $? -eq 0 ] || { echo "ERROR: Failed to create spinta database"; exit 1; }

psql -h localhost -p 54321 -U admin spinta <<EOF
BEGIN TRANSACTION;
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF
[ $? -eq 0 ] || { echo "ERROR: Failed to setup spinta extensions"; exit 1; }

echo "--- Activating poetry virtual environment ---"
# Activate the poetry virtual environment in the current shell
eval $(poetry env activate)
[ $? -eq 0 ] || { echo "ERROR: Failed to activate poetry env"; exit 1; }

echo "--- Setting up instance directory for $NEW_VERSION ---"
# Set up instance directory for this release version
INSTANCE=releases/$NEW_VERSION
BASEDIR=$PWD/var/instances/$INSTANCE

mkdir -p "$BASEDIR"
[ $? -eq 0 ] || { echo "ERROR: Failed to create directory $BASEDIR"; exit 1; }

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

# Verify config.yml was written and contains the expected values
[ -f "$BASEDIR/config.yml" ] || { echo "ERROR: config.yml was not created"; exit 1; }

config=$(cat "$BASEDIR/config.yml")
echo "$config" | grep -q "env: production" || { echo "ERROR: config.yml missing 'env: production'"; exit 1; }
echo "$config" | grep -q "dsn: postgresql://admin:admin123@localhost:54321/spinta" || { echo "ERROR: config.yml missing backend dsn"; exit 1; }
echo "$config" | grep -q "path: $BASEDIR/manifest.csv" || { echo "ERROR: config.yml missing manifest path"; exit 1; }

# Point spinta to the instance config for all subsequent commands
export SPINTA_CONFIG=$BASEDIR/config.yml

echo "--- Updating manifest repo and copying datasets ---"
# Switch to the manifest repo, update it, validate all datasets,
# and copy the relevant ones into this release's manifest.csv
(
  cd /home/karina/work/vssa/manifest

  git status

  git checkout get.data.gov.lt
  [ $? -eq 0 ] || { echo "ERROR: Failed to checkout get.data.gov.lt"; exit 1; }

  git pull
  [ $? -eq 0 ] || { echo "ERROR: Failed to pull manifest"; exit 1; }

  # Validate all dataset CSV files in the manifest repo
  find datasets -iname "*.csv" | xargs spinta check
  [ $? -eq 0 ] || { echo "ERROR: spinta check failed"; exit 1; }

  # Copy the datasets listed in get_data_gov_lt.in into the release manifest
  cat get_data_gov_lt.in | xargs spinta copy -o "$BASEDIR"/manifest.csv
  [ $? -eq 0 ] || { echo "ERROR: spinta copy failed"; exit 1; }

  # Preview the manifest structure for the datasets used in smoke tests
  spinta show \
      datasets/gov/rc/ar/adresai.csv \
      datasets/gov/rc/jar/formos_statusai.csv \
      datasets/gov/rc/jar/iregistruoti.csv
  [ $? -eq 0 ] || { echo "ERROR: spinta show failed"; exit 1; }
)

echo "--- Validating manifest against instance config ---"
# Validate the copied manifest against the spinta instance config
spinta check
[ $? -eq 0 ] || { echo "ERROR: spinta check failed"; exit 1; }

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

echo "--- Validating smoke test manifest ---"
# Validate the smoke test manifest (sdsa.txt) before running migrations
spinta check "$BASEDIR"/sdsa.txt
[ $? -eq 0 ] || { echo "ERROR: spinta check sdsa.txt failed"; exit 1; }

echo "--- Running database migrations and bootstrap ---"
# Run database migrations to apply the manifest schema to the internal database
spinta --tb native migrate --autocommit
[ $? -eq 0 ] || { echo "ERROR: spinta migrate failed"; exit 1; }

# Bootstrap creates internal metadata tables required by spinta
spinta --tb native bootstrap
[ $? -eq 0 ] || { echo "ERROR: spinta bootstrap failed"; exit 1; }

# List tables to confirm the migration created the expected schema
export PAGER="cat"
psql -h localhost -p 54321 -U admin spinta -c '\dt public.*'
[ $? -eq 0 ] || { echo "ERROR: Failed to list spinta tables"; exit 1; }

echo "--- Starting internal server (waiting 30s for startup) ---"
# Start the spinta server in INTERNAL mode and wait for it to be ready
test -n "$PID" && kill "$PID"
spinta run &>> "$BASEDIR"/spinta.log & PID=$!
sleep 30
tail -50 "$BASEDIR"/spinta.log

# Verify the server is up and responding
http :8000
[ $? -eq 0 ] || { echo "ERROR: Server not responding on :8000"; exit 1; }

http :8000/version
[ $? -eq 0 ] || { echo "ERROR: /version endpoint failed"; exit 1; }

http :8000/datasets/gov | jq -c '._data[]'
[ $? -eq 0 ] || { echo "ERROR: /datasets/gov endpoint failed"; exit 1; }

echo "--- Obtaining auth token ---"
# Obtain an auth token and insert test data into the internal server
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
# Fail early if token is empty — likely wrong credentials or server not ready
[ -n "$TOKEN" ] || { echo "ERROR: Failed to obtain auth token"; exit 1; }
echo "$AUTH"

echo "--- Inserting test Adresas record ---"
# Insert a test Adresas record that will later be referenced by JuridinisAsmuo via keymap sync
http POST :8000/datasets/gov/rc/ar/adresai/Adresas "$AUTH" <<EOF
{
    "_id": "264ae0f9-53eb-496b-a07c-ce1b9cbe510c",
    "tipas": 2,
    "aob_kodas": 190557457,
    "aob_data_nuo": "2018-12-07"
}
EOF
[ $? -eq 0 ] || { echo "ERROR: Failed to POST Adresas"; exit 1; }

echo "--- Starting external server on port 7000 (waiting 10s for startup) ---"
# Start the spinta server in EXTERNAL mode on port 7000 (internal stays on 8000)
test -n "$EXTERNAL_PID" && kill "$EXTERNAL_PID"
spinta run --port 7000 --mode external "$BASEDIR"/sdsa.txt &>> "$BASEDIR"/spinta.log & EXTERNAL_PID=$!
sleep 10
tail -50 "$BASEDIR"/spinta.log

# Forma and Statusas come from the external database — they should work without errors
formos=$(http :7000/datasets/gov/rc/jar/formos_statusai/Forma)
echo "$formos" | grep -q "errors\|error" && { echo "ERROR: Forma returned unexpected error"; exit 1; }

statusas=$(http :7000/datasets/gov/rc/jar/formos_statusai/Statusas)
echo "$statusas" | grep -q "errors\|error" && { echo "ERROR: Statusas returned unexpected error"; exit 1; }

# JuridinisAsmuo references Adresas which is in INTERNAL mode — an error here is expected
juridinis=$(http :7000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo)
echo "$juridinis" | grep -q "errors\|error" && echo "OK: JuridinisAsmuo returned expected error (Adresas in internal mode)" || echo "WARNING: JuridinisAsmuo did not return expected error"

echo "--- Syncing keymap from internal server ---"
# Sync the keymap from the internal server so external mode can resolve Adresas references
spinta keymap sync "$BASEDIR"/sdsa.txt -i test@localhost
[ $? -eq 0 ] || { echo "ERROR: keymap sync failed"; exit 1; }

# After sync, JuridinisAsmuo should resolve adresas._id correctly
juridinis_synced=$(http :7000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo)
echo "$juridinis_synced" | grep -q "264ae0f9-53eb-496b-a07c-ce1b9cbe510c" || { echo "ERROR: adresas._id not found after keymap sync"; exit 1; }
echo "OK: JuridinisAsmuo returns correct adresas._id after sync"

# Stop the external server — no longer needed
test -n "$EXTERNAL_PID" && kill "$EXTERNAL_PID"

echo "--- Running smoke tests: pushing data from external to internal ---"
# Smoke tests: clear local state and push data from external source to internal server
test -f "$BASEDIR"/keymap.db && rm "$BASEDIR"/keymap.db
test -f "$BASEDIR"/push/localhost.db && rm "$BASEDIR"/push/localhost.db

spinta push "$BASEDIR"/sdsa.txt -o test@localhost
[ $? -eq 0 ] || { echo "ERROR: spinta push failed"; exit 1; }

echo "--- Verifying response formats (JSON, CSV, ASCII, RDF) ---"
# Verify the pushed data is queryable in multiple output formats
juridinis_json=$(http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(json)")
echo "$juridinis_json" | grep -q "188772433" || { echo "ERROR: JSON response missing expected ja_kodas"; exit 1; }

juridinis_csv=$(http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(csv)")
echo "$juridinis_csv" | grep -q "188772433" || { echo "ERROR: CSV response missing expected ja_kodas"; exit 1; }

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(ascii)"
[ $? -eq 0 ] || { echo "ERROR: ASCII format request failed"; exit 1; }

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)&format(rdf)"
[ $? -eq 0 ] || { echo "ERROR: RDF format request failed"; exit 1; }

http GET ":8000/datasets/gov/rc/jar/:all?format(rdf)"
[ $? -eq 0 ] || { echo "ERROR: RDF all request failed"; exit 1; }

http GET ":8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo/:changes"
[ $? -eq 0 ] || { echo "ERROR: changes request failed"; exit 1; }

# Print URLs for manual browser inspection if needed
echo "http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo"
echo "http://localhost:8000/datasets/gov/rc/jar/iregistruoti/JuridinisAsmuo?select(_id,ja_kodas,ja_pavadinimas,reg_data,forma.pavadinimas,statusas.pavadinimas)"
echo "http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/264ae0f9-53eb-496b-a07c-ce1b9cbe510c"
echo "http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/:changes"
echo "http://localhost:8000/datasets/gov/rc/ar/adresai/Adresas/264ae0f9-53eb-496b-a07c-ce1b9cbe510c/:changes"

echo "--- Updating release date in CHANGES.rst ---"
# Replace "unreleased" with today's date in CHANGES.rst now that we're confident the release is good
ed CHANGES.rst <<EOF
/$NEW_VERSION (unreleased)/c
$NEW_VERSION ($(date +%Y-%m-%d))
.
wq
EOF
[ $? -eq 0 ] || { echo "ERROR: Failed to update release date in CHANGES.rst"; exit 1; }


# Prompt before publishing — this is irreversible once pushed to PyPI
read -n1 -p "Run poetry build and publish? (y/n) "
echo
if [ "$REPLY" = "y" ]; then
    echo "--- Building distribution packages ---"
    # Build the distribution packages (wheel + sdist)
    poetry build
    [ $? -eq 0 ] || { echo "ERROR: poetry build failed"; exit 1; }

    echo "--- Publishing to PyPI ---"
    # Upload the built packages to PyPI
    poetry publish
    [ $? -eq 0 ] || { echo "ERROR: poetry publish failed"; exit 1; }

    echo "--- Generating hashed requirements file ---"
    # Export pinned requirements for this release version
    poetry export -f requirements.txt \
      --output requirements/spinta-${NEW_VERSION}.txt
    [ $? -eq 0 ] || { echo "ERROR: poetry export failed"; exit 1; }

    # Build the header line for spinta itself with its PyPI hashes
    echo "spinta==${NEW_VERSION}; python_version >= \"3.10\" and python_version < \"4.0\" \\" > spinta-header.txt

    # Fetch sha256 hashes for the published release from PyPI and append to header
    curl -s https://pypi.org/pypi/spinta/${NEW_VERSION}/json | \
      jq -r '.urls[] | "--hash=sha256:\(.digests.sha256)"' \
      | sed 's/^/    /' >> spinta-header.txt
    [ $? -eq 0 ] || { echo "ERROR: Failed to fetch spinta hashes from PyPI"; exit 1; }

    # Prepend the spinta hash header to the exported requirements file
    cat spinta-header.txt requirements/spinta-${NEW_VERSION}.txt > spinta-merged.txt
    mv spinta-merged.txt requirements/spinta-${NEW_VERSION}.txt
    [ $? -eq 0 ] || { echo "ERROR: Failed to prepend hashes to requirements file"; exit 1; }

    # Also update the rolling "latest pre-release" requirements file
    cp requirements/spinta-${NEW_VERSION}.txt requirements/spinta-latest-pre.txt
    [ $? -eq 0 ] || { echo "ERROR: Failed to copy requirements to spinta-latest-pre.txt"; exit 1; }

    git add requirements/spinta-${NEW_VERSION}.txt
    git commit -am "Add hashed requirements for ${NEW_VERSION} and update latest"
    [ $? -eq 0 ] || { echo "ERROR: git commit failed"; exit 1; }

    git push
    [ $? -eq 0 ] || { echo "ERROR: git push failed"; exit 1; }

    echo "--- Tagging release $NEW_VERSION ---"
    # Tag the release commit so it is easy to find in git history
    git tag -a $NEW_VERSION -m "Releasing version $NEW_VERSION"
    [ $? -eq 0 ] || { echo "ERROR: git tag failed"; exit 1; }

    git push origin $NEW_VERSION
    [ $? -eq 0 ] || { echo "ERROR: git push tag failed"; exit 1; }

    echo "--- Preparing for next version $FUTURE_VERSION ---"
    # Bump the version in pyproject.toml to the next development version
    ed pyproject.toml <<EOF
    /^version = /c
    version = "$FUTURE_VERSION"
    .
    wq
    EOF
    [ $? -eq 0 ] || { echo "ERROR: Failed to update version in pyproject.toml"; exit 1; }

    # Add a new unreleased section to CHANGES.rst for the next version
    ed CHANGES.rst <<EOF
    /^###/a

    $FUTURE_VERSION (unreleased)
    =====================

    .
    wq
    EOF
    [ $? -eq 0 ] || { echo "ERROR: Failed to update CHANGES.rst for future version"; exit 1; }

    # Preview the top of CHANGES.rst and the diff before committing
    head CHANGES.rst
    git diff
    git commit -a -m "Prepare for the next $FUTURE_VERSION release"
    [ $? -eq 0 ] || { echo "ERROR: git commit for future version failed"; exit 1; }

    git push origin HEAD
    [ $? -eq 0 ] || { echo "ERROR: git push for future version failed"; exit 1; }

    git log -n3

    # Return to master now that the release branch work is complete
    git checkout master
    [ $? -eq 0 ] || { echo "ERROR: Failed to checkout master"; exit 1; }

else
    echo "Skipping poetry build and publish"
fi

echo "--- Shutting down Docker containers ---"
# Shut down the docker containers
docker compose down
[ $? -eq 0 ] || { echo "ERROR: docker compose down failed"; exit 1; }

echo "=== Release $NEW_VERSION complete! ==="

