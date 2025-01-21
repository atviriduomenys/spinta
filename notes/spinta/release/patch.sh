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

export CURRENT_PATCH=81
export NEW_PATCH=82
export FUTURE_PATCH=83

export CURRENT_VERSION=$RELEASE_VERSION.$CURRENT_PATCH
export NEW_VERSION=$RELEASE_VERSION.$NEW_PATCH
export FUTURE_VERSION=$RELEASE_VERSION.$FUTURE_PATCH

export PREPARE_BRANCH=prepare_${NEW_VERSION}_version
git branch $PREPARE_BRANCH
git checkout $PREPARE_BRANCH
git status

# notes/spinta/release/common.sh    Check outdated packages and upgrade them
# Install
#| Package operations: 1 install, 10 updates, 0 removals
#|
#|   • Updating charset-normalizer (3.4.0 -> 3.4.1)
#|   • Updating jinja2 (3.1.4 -> 3.1.5)
#|   • Updating pygments (2.18.0 -> 2.19.0)
#|   • Updating click (8.1.7 -> 8.1.8)
#|   • Updating fsspec (2024.10.0 -> 2024.12.0)
#|   • Updating livereload (2.7.0 -> 2.7.1)
#|   • Updating mypy (1.13.0 -> 1.14.1)
#|   • Updating psutil (6.1.0 -> 6.1.1)
#|   • Installing cachetools (5.5.0)
#|   • Updating ruamel-yaml (0.18.6 -> 0.18.9)
#|   • Updating starlette (0.42.0 -> 0.44.0)

# Update
#| Package operations: 0 installs, 7 updates, 0 removals
#|
#|   • Updating pygments (2.19.0 -> 2.19.1)
#|   • Updating prompt-toolkit (3.0.48 -> 3.0.50)
#|   • Updating cloudpickle (3.1.0 -> 3.1.1)
#|   • Updating geoalchemy2 (0.16.0 -> 0.17.0)
#|   • Updating phonenumbers (8.13.52 -> 8.13.53)
#|   • Updating responses (0.25.3 -> 0.25.6)
#|   • Updating ruamel-yaml (0.18.9 -> 0.18.10)

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
#| 2278 passed, 45 skipped, 57 warnings in 359.62s (0:05:59)

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
#| (3406 rows)

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
