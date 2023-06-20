cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
head CHANGES.rst

docker-compose ps
docker-compose up -d
unset SPINTA_CONFIG
poetry run pytest -vvx --tb=short tests
#| 1218 passed, 34 skipped, 5 warnings in 206.86s (0:03:26)
docker-compose down

# Current version
version=0.1.50
xdg-open https://github.com/atviriduomenys/spinta/compare/$version..master
xdg-open https://github.com/atviriduomenys/spinta/compare/$version...master
# Update CHANGES.rst
poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

# New version
version=0.1.51
ed pyproject.toml <<EOF
/^version = /c
version = "$version"
.
wq
EOF
ed CHANGES.rst <<EOF
/unreleased/c
$version ($(date +%Y-%m-%d))
.
wq
EOF
git diff

poetry build
poetry publish
xdg-open https://pypi.org/project/spinta/
git commit -a -m "Releasing version $version"
git push origin master
git tag -a $version -m "Releasing version $version"
git push origin $version

# Next version (after new one)
version=0.1.52
ed pyproject.toml <<EOF
/^version = /c
version = "$version.dev0"
.
wq
EOF
ed CHANGES.rst <<EOF
/^###/a

$version (unreleased)
===================

.
wq
EOF
head CHANGES.rst
git diff
git commit -a -m "Prepare for the next $version release"
git push origin master
git log -n3
