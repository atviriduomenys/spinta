cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
CURRENT_VERSION=0.1.52
NEXT_VERSION=0.1.53
FUTURE_VERSION=0.1.54

head CHANGES.rst

# Check what was changed and update CHANGES.rst
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION..master
xdg-open https://github.com/atviriduomenys/spinta/compare/$CURRENT_VERSION...master
# Update CHANGES.rst

docker-compose ps
docker-compose up -d
unset SPINTA_CONFIG
poetry run pytest -vvx --tb=short tests
#| 1232 passed, 34 skipped, 8 warnings in 205.29s (0:03:25)
docker-compose down

poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

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
