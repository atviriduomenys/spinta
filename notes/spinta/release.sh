cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
export CURRENT_VERSION=0.1.52
export NEXT_VERSION=0.1.53
export FUTURE_VERSION=0.1.54

head CHANGES.rst

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION..master
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master
# Update CHANGES.rst

docker-compose ps
docker-compose up -d
unset SPINTA_CONFIG
poetry run pytest -vvx --tb=short tests
#| 1232 passed, 34 skipped, 8 warnings in 203.33s (0:03:23)

poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

# Check if new Spinta version works with manifest files
poetry shell

# Configure Spinta server instance
INSTANCE=releases/$NEXT_VERSION
BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR
cat $BASEDIR/config.yml
cat > $BASEDIR/config.yml <<EOF
env: production
data_path: $PWD/$BASEDIR
default_auth_client: default

keymaps:
  default:
    type: sqlalchemy
    dsn: sqlite:///$PWD/$BASEDIR/keymap.db

backends:
  default:
    type: postgresql
    dsn: postgresql://admin:admin123@localhost:54321/spinta

manifest: default
manifests:
  default:
    type: csv
    path: $PWD/$BASEDIR/manifest.csv
    backend: default
    keymap: default
    mode: internal

accesslog:
  type: file
  file: $PWD/$BASEDIR/accesslog.json
EOF
export SPINTA_CONFIG=$BASEDIR/config.yml

tree ~/.config/spinta/
poetry run spinta genkeys

# Add default client
cat ~/.config/spinta/clients/test.yml
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
psql -h localhost -p 54321 -U admin spinta <<EOF
BEGIN TRANSACTION;
  DROP SCHEMA "public" CASCADE;
  CREATE SCHEMA "public";
  CREATE EXTENSION IF NOT EXISTS postgis;
  CREATE EXTENSION IF NOT EXISTS postgis_topology;
  CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
  CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
COMMIT;
EOF

# Run migrations
spinta bootstrap

# Run server
test -n "$PID" && kill $PID
spinta run &>> $BASEDIR/spinta.log &; PID=$!
tail -50 $BASEDIR/spinta.log

# Check if server works
http :8000
http :8000/version

xdg-open http://localhost:8000/datasets/gov

test -n "$PID" && kill $PID
unset SPINTA_CONFIG
cd -
docker-compose down
deactivate


ed pyproject.toml <<EOF
/^version = /c
version = "$NEXT_VERSION"
.
wq
EOF
ed CHANGES.rst <<EOF
/unreleased/c
$NEXT_VERSION ($(date +%Y-%m-%d))
.
wq
EOF
git diff

poetry build
poetry publish
xdg-open https://pypi.org/project/spinta/
git commit -a -m "Releasing version $NEXT_VERSION"
git push origin master
git tag -a $NEXT_VERSION -m "Releasing version $NEXT_VERSION"
git push origin $NEXT_VERSION

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
