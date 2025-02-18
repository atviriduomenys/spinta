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

export CURRENT_PATCH=82
export NEW_PATCH=83
export FUTURE_PATCH=84

export CURRENT_VERSION=$RELEASE_VERSION.$CURRENT_PATCH
export NEW_VERSION=$RELEASE_VERSION.$NEW_PATCH
export FUTURE_VERSION=$RELEASE_VERSION.$FUTURE_PATCH

export PREPARE_BRANCH=prepare_${NEW_VERSION}_version
git branch $PREPARE_BRANCH
git checkout $PREPARE_BRANCH
git status

# notes/spinta/release/common.sh    Check outdated packages and upgrade them
# Install
#| Package operations: 0 installs, 39 updates, 0 removals
#|
#|   • Updating markupsafe (3.0.2 -> 2.1.5)
#|   • Updating urllib3 (2.3.0 -> 2.2.3)
#|   • Updating alabaster (0.7.16 -> 0.7.13)
#|   • Updating docutils (0.21.2 -> 0.20.1)
#|   • Updating sphinxcontrib-applehelp (2.0.0 -> 1.0.4)
#|   • Updating sphinxcontrib-devhelp (2.0.0 -> 1.0.2)
#|   • Updating sphinxcontrib-htmlhelp (2.1.0 -> 2.0.1)
#|   • Updating sphinxcontrib-qthelp (2.0.0 -> 1.0.3)
#|   • Updating sphinxcontrib-serializinghtml (2.0.0 -> 1.1.5)
#|   • Updating numpy (2.0.2 -> 1.24.4)
#|   • Updating prompt-toolkit (3.0.48 -> 3.0.50)
#|   • Updating propcache (0.2.1 -> 0.2.0)
#|   • Updating sphinx (7.4.7 -> 7.1.2)
#|   • Updating zipp (3.21.0 -> 3.20.2)
#|   • Updating aiosignal (1.3.2 -> 1.3.1)
#|   • Updating anyio (4.8.0 -> 4.5.2)
#|   • Updating cloudpickle (3.1.0 -> 3.1.1)
#|   • Updating coverage (7.6.10 -> 7.6.1)
#|   • Updating dnspython (2.7.0 -> 2.6.1)
#|   • Updating ipython (8.18.1 -> 8.12.3)
#|   • Updating isodate (0.7.2 -> 0.6.1)
#|   • Updating pandas (2.2.3 -> 2.0.3)
#|   • Updating partd (1.4.2 -> 1.4.1)
#|   • Updating pyparsing (3.2.1 -> 3.1.4)
#|   • Updating ruamel-yaml-clib (0.2.12 -> 0.2.8)
#|   • Updating setuptools (75.7.0 -> 75.3.0)
#|   • Updating yarl (1.18.3 -> 1.15.2)
#|   • Updating aiohttp (3.11.11 -> 3.10.11)
#|   • Updating dask (2024.8.0 -> 2023.5.0)
#|   • Updating geoalchemy2 (0.16.0 -> 0.17.0)
#|   • Updating objprint (0.3.0 -> 0.2.3)
#|   • Updating phonenumbers (8.13.52 -> 8.13.53)
#|   • Updating pyproj (3.6.1 -> 3.5.0)
#|   • Updating pytest-cov (6.0.0 -> 5.0.0)
#|   • Updating rdflib (7.1.1 -> 6.3.2)
#|   • Updating responses (0.25.3 -> 0.25.6)
#|   • Updating sphinx-autobuild (2024.10.3 -> 2021.3.14)
#|   • Updating starlette (0.45.2 -> 0.44.0)
#|   • Updating uvicorn (0.34.0 -> 0.33.0)

# Update
#| Package operations: 0 installs, 15 updates, 0 removals
#|
#|   • Updating certifi (2024.12.14 -> 2025.1.31)
#|   • Updating babel (2.16.0 -> 2.17.0)
#|   • Updating executing (2.1.0 -> 2.2.0)
#|   • Updating pytz (2024.2 -> 2025.1)
#|   • Updating tzdata (2024.2 -> 2025.1)
#|   • Updating attrs (24.3.0 -> 25.1.0)
#|   • Updating fsspec (2024.12.0 -> 2025.2.0)
#|   • Updating mako (1.3.8 -> 1.3.9)
#|   • Updating psutil (6.1.1 -> 7.0.0)
#|   • Updating cachetools (5.5.0 -> 5.5.1)
#|   • Updating geoalchemy2 (0.17.0 -> 0.17.1)
#|   • Updating lxml (5.3.0 -> 5.3.1)
#|   • Updating phonenumbers (8.13.53 -> 8.13.55)
#|   • Updating shapely (2.0.6 -> 2.0.7)
#|   • Updating xlsxwriter (3.2.0 -> 3.2.2)

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
#| 2339 passed, 45 skipped, 58 warnings in 392.46s (0:06:32)

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
#| (3464 rows)

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

# Create pull request for release version in github and check if all tests run

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

# Merge pull request with release branch

# Prepare master branch post release
git status
git checkout master
git pull

export POST_RELEASE_BRANCH=post-release_${NEW_VERSION}
git branch $POST_RELEASE_BRANCH
git checkout $POST_RELEASE_BRANCH
git status


# Update version release date in CHANGES.rst
ed CHANGES.rst <<EOF
/$NEW_VERSION (unreleased)/c
$NEW_VERSION ($(date +%Y-%m-%d))
.
wq
EOF

ed CHANGES.rst <<EOF
/$NEW_VERSION ($(date +%Y-%m-%d))/i
$FUTURE_VERSION (unreleased)
===================


.
wq
EOF
head CHANGES.rst

git diff
git commit -a -m "Post-release changes for $NEW_VERSION release"
git push origin HEAD
git log -n3

# Create PR for master and merge it if all tests pass
