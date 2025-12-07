cd ~/dev/data/spinta

# Reset config paths and turn off running instance
unset SPINTA_CONFIG
unset SPINTA_CONFIG_PATH
test -n "$PID" && kill "$PID"

# Setup versions and create prepare branch
export MAJOR=0
export MINOR=2dev11
export OLD_MINOR=2dev10
export FUTURE_MINOR=2dev12
export RELEASE_VERSION=$MAJOR.$MINOR
export CURRENT_VERSION=$MAJOR.$OLD_MINOR
export FUTURE_VERSION=$MAJOR.$FUTURE_MINOR
export NEW_VERSION=$RELEASE_VERSION

git status
git checkout master
git pull
git tag -l -n1 | sort -h | tail -n5

export PREPARE_BRANCH=release_${NEW_VERSION}_version
git branch $PREPARE_BRANCH
git checkout $PREPARE_BRANCH
git status


# notes/spinta/release/common.sh    Check outdated packages and upgrade them
# Install
#| No dependencies to install or update

# Update
#| Package operations: 0 installs, 16 updates, 0 removals
#|
#|   • Updating jinja2 (3.1.5 -> 3.1.6)
#|   • Updating typing-extensions (4.12.2 -> 4.13.1)
#|   • Updating decorator (5.1.1 -> 5.2.1)
#|   • Updating iniconfig (2.0.0 -> 2.1.0)
#|   • Updating pytz (2025.1 -> 2025.2)
#|   • Updating tzdata (2025.1 -> 2025.2)
#|   • Updating attrs (25.1.0 -> 25.3.0)
#|   • Updating fsspec (2025.2.0 -> 2025.3.0)
#|   • Updating rich (13.9.4 -> 14.0.0)
#|   • Updating setuptools (75.3.0 -> 75.3.2)
#|   • Updating cachetools (5.5.1 -> 5.5.2)
#|   • Updating lxml (5.3.1 -> 5.3.2)
#|   • Updating phonenumbers (8.13.55 -> 9.0.2)
#|   • Updating responses (0.25.6 -> 0.25.7)
#|   • Updating setuptools-scm (8.1.0 -> 8.2.0)
#|   • Updating typer (0.15.1 -> 0.15.2)


# Run Makefile
(cd docs && make upgrade)

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master
head CHANGES.rst
# Update CHANGES.rst
# notes/spinta/release/common.sh    Generate and check changes and readme html files

# notes/docker.sh                   Start docker compose
# notes/spinta/release/common.sh    Reset test database

poetry run pytest -vvx --tb=short tests
#| 2394 passed, 45 skipped, 57 warnings in 400.85s (0:06:40)

# If possible run same tests using test and prod env library versions
# Test env
# - poetry run pytest -vvx --tb=short tests
#| Did not run this time
# Prod env
# - poetry run pytest -vvx --tb=short tests
#| Did not run this time

# Check if new Spinta version works with manifest files
poetry env activate
# arba `poetry shell` priklausomai nuo Poetry versijos

# Configure Spinta server instance
INSTANCE=releases/$NEW_VERSION
BASEDIR=$PWD/var/instances/$INSTANCE

# notes/spinta/release/common.sh    Reset EXTERNAL (source) database
# notes/spinta/release/common.sh    Reset INTERNAL database

# notes/spinta/release/common.sh    Configure spinta
# notes/spinta/release/common.sh    Create manifest file

# notes/spinta/release/common.sh    Run migrations
#| (3624 rows)

# notes/spinta/release/common.sh    Run server in INTERNAL mode
# Don't forget to add client to server and credentials;
# - notes/spinta/server.sh
# - notes/spinta/push.sh
# Add testing data

# notes/spinta/release/common.sh    Run server in EXTERNAL mode
# Sync INTERNAL model data

# notes/spinta/release/common.sh    Run smoke tests

test -n "$PID" && kill "$PID"
unset SPINTA_CONFIG

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


# generate hashed requirements file

poetry export -f requirements.txt \
  --output requirements/spinta-${NEW_VERSION}.txt

# get hashes to spinta itself

echo "spinta==${NEW_VERSION} \\" > spinta-header.txt

curl -s https://pypi.org/pypi/spinta/${NEW_VERSION}/json | \
  jq -r '.urls[] | "--hash=sha256:\(.digests.sha256)"' \
  | sed 's/^/    /' >> spinta-header.txt


# ADD THOSE HASHES to the file manually

cp requirements/spinta-${NEW_VERSION}.txt requirements/spinta-latest-pre.txt

git add requirements/spinta-${NEW_VERSION}.txt
git commit -am "Add hashed requirements for ${NEW_VERSION} and update latest"
git push



# Prepare pyproject.toml and CHANGES.rst for future versions
git tag -a $NEW_VERSION -m "Releasing version $NEW_VERSION"
git push origin $NEW_VERSION

ed pyproject.toml <<EOF
/^version = /c
version = "$FUTURE_VERSION"
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
