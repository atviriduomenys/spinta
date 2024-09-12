cd ~/dev/data/spinta

# Reset config paths and turn off running instance
unset SPINTA_CONFIG
unset SPINTA_CONFIG_PATH
test -n "$PID" && kill "$PID"

# Setup versions and create prepare branch
export MAJOR=0
export MINOR=1
export RELEASE_VERSION=$MAJOR.$MINOR

git status
git checkout $RELEASE_VERSION
git pull
git tag -l -n1 | sort -h | tail -n5

export CURRENT_PATCH=70
export NEW_PATCH=71
export FUTURE_PATCH=72

export CURRENT_VERSION=$RELEASE_VERSION.$CURRENT_PATCH
export NEW_VERSION=$RELEASE_VERSION.$NEW_PATCH
export FUTURE_VERSION=$RELEASE_VERSION.$FUTURE_PATCH

export PREPARE_BRANCH=prepare_${NEW_VERSION}_version
git branch $PREPARE_BRANCH
git checkout $PREPARE_BRANCH
git status


# notes/spinta/release/common.sh    Check outdated packages and upgrade them
#| Package operations: 0 installs, 17 updates, 0 removals
#|
#|   • Updating certifi (2024.7.4 -> 2024.8.30)
#|   • Updating executing (2.0.1 -> 2.1.0)
#|   • Updating cffi (1.17.0 -> 1.17.1)
#|   • Updating greenlet (3.0.3 -> 3.1.0)
#|   • Updating multidict (6.0.5 -> 6.1.0)
#|   • Updating pytz (2024.1 -> 2024.2)
#|   • Updating cryptography (43.0.0 -> 43.0.1)
#|   • Updating fsspec (2024.6.1 -> 2024.9.0)
#|   • Updating importlib-metadata (8.4.0 -> 8.5.0)
#|   • Updating rich (13.8.0 -> 13.8.1)
#|   • Updating setuptools (73.0.1 -> 74.1.2)
#|   • Updating sqlalchemy (1.4.53 -> 1.4.54)
#|   • Updating yarl (1.9.4 -> 1.11.1)
#|   • Updating httpx (0.27.1 -> 0.27.2)
#|   • Updating msgpack (1.0.8 -> 1.1.0)
#|   • Updating phonenumbers (8.13.44 -> 8.13.45)
#|   • Updating starlette (0.38.2 -> 0.38.5)

# Run Makefile
cd docs
make upgrade
cd ..

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...$RELEASE_VERSION
head CHANGES.rst
# Update CHANGES.rst
# notes/spinta/release/common.sh    Generate and check changes and readme html files

# notes/docker.sh                   Start docker compose
# notes/spinta/release/common.sh    Reset test database

poetry run pytest -vvx --tb=short tests
#| 2067 passed, 42 skipped, 397 warnings in 311.51s (0:05:11)

# If possible run same tests using test and prod env library versions
# Test env
# - poetry run pytest -vvx --tb=short tests
#| Did not run this time
# Prod env
# - poetry run pytest -vvx --tb=short tests
#| Did not run this time

# Check if new Spinta version works with manifest files
poetry shell

# Configure Spinta server instance
INSTANCE=releases/$NEW_VERSION
BASEDIR=$PWD/var/instances/$INSTANCE

# notes/spinta/release/common.sh    Reset EXTERNAL (source) database
# notes/spinta/release/common.sh    Reset INTERNAL database

# notes/spinta/release/common.sh    Configure spinta
# notes/spinta/release/common.sh    Create manifest file

# notes/spinta/release/common.sh    Run server in EXTERNAL mode
# notes/spinta/release/common.sh    Run migrations
#| (2912 rows)

# notes/spinta/release/common.sh    Run server in INTERNAL mode
# Don't forget to add client to server and credentials;
# - notes/spinta/server.sh
# - notes/spinta/push.sh

# notes/spinta/release/common.sh    Run smoke tests

test -n "$PID" && kill "$PID"
unset SPINTA_CONFIG
exit
# notes/docker.sh                   Shutdown docker compose

# Update project version in pyproject.toml
cd ~/dev/data/spinta

ed pyproject.toml <<EOF
/^version = /c
version = "$NEW_VERSION"
.
wq
EOF

# Update version release date in CHANGES.rst
ed CHANGES.rst <<EOF
/unreleased/c
$NEW_VERSION ($(date +%Y-%m-%d))
.
wq
EOF
git diff

git commit -a -m "Releasing version $NEW_VERSION"
git push origin HEAD

# Create pull request for release version and master in github and check if all tests run

# notes/spinta/release/common.sh    Publish version to PyPI

# Prepare pyproject.toml and CHANGES.rst for future versions
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
git push origin HEAD
git log -n3

# Merge pull request with release and master branches
