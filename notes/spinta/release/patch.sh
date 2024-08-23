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

export CURRENT_PATCH=67
export NEW_PATCH=68
export FUTURE_PATCH=69

export CURRENT_VERSION=$RELEASE_VERSION.$CURRENT_PATCH
export NEW_VERSION=$RELEASE_VERSION.$NEW_PATCH
export FUTURE_VERSION=$RELEASE_VERSION.$FUTURE_PATCH

export PREPARE_BRANCH=prepare_${NEW_VERSION}_version
git branch $PREPARE_BRANCH
git checkout $PREPARE_BRANCH
git status


# notes/spinta/release/common.sh    Check outdated packages and upgrade them
#Package operations: 0 installs, 18 updates, 0 removals
#
#  • Updating babel (2.15.0 -> 2.16.0)
#  • Updating cffi (1.16.0 -> 1.17.0)
#  • Updating zipp (3.19.2 -> 3.20.0)
#  • Updating aiohappyeyeballs (2.3.4 -> 2.4.0)
#  • Updating attrs (23.2.0 -> 24.2.0)
#  • Updating cheap-repr (0.5.1 -> 0.5.2)
#  • Updating coverage (7.6.0 -> 7.6.1)
#  • Updating importlib-metadata (8.2.0 -> 8.4.0)
#  • Updating pyyaml (6.0.1 -> 6.0.2)
#  • Updating setuptools (72.1.0 -> 73.0.1)
#  • Updating aiohttp (3.10.0 -> 3.10.5)
#  • Updating gunicorn (22.0.0 -> 23.0.0)
#  • Updating lxml (5.2.2 -> 5.3.0)
#  • Updating phonenumbers (8.13.42 -> 8.13.43)
#  • Updating shapely (2.0.5 -> 2.0.6)
#  • Updating tqdm (4.66.4 -> 4.66.5)
#  • Updating typer (0.12.3 -> 0.12.4)
#  • Updating uvicorn (0.30.5 -> 0.30.6)

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

# Fixed 735 external backend support

poetry run pytest -vvx --tb=short tests
#| 2045 passed, 42 skipped, 397 warnings in 320.08s (0:05:20)

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
#| (2864 rows)

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
