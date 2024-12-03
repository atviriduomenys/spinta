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

export CURRENT_PATCH=79
export NEW_PATCH=80
export FUTURE_PATCH=81

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
#|   • Updating asttokens (2.4.1 -> 3.0.0)
#|   • Updating tomli (2.1.0 -> 2.2.1)
#|   • Updating tornado (6.4.1 -> 6.4.2)
#|   • Updating aiohappyeyeballs (2.4.3 -> 2.4.4)
#|   • Updating async-timeout (4.0.3 -> 5.0.1)
#|   • Updating httpcore (1.0.6 -> 1.0.7)
#|   • Updating aiohttp (3.10.10 -> 3.10.11)
#|   • Updating geoalchemy2 (0.15.2 -> 0.16.0)
#|   • Updating httpx (0.27.2 -> 0.28.0)
#|   • Updating phonenumbers (8.13.49 -> 8.13.51)
#|   • Updating python-multipart (0.0.17 -> 0.0.19)
#|   • Updating sphinx-rtd-theme (3.0.1 -> 3.0.2)
#|   • Updating sqlparse (0.5.1 -> 0.5.2)
#|   • Updating starlette (0.41.2 -> 0.41.3)
#|   • Updating tqdm (4.67.0 -> 4.67.1)
#|   • Updating typer (0.13.0 -> 0.14.0)
#|   • Updating uvicorn (0.32.0 -> 0.32.1)


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
#| 2224 passed, 45 skipped, 55 warnings in 354.33s (0:05:54)

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
#| (3248 rows)

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
