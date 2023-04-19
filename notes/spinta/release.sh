cd ~/dev/data/spinta
git status
git checkout master
git pull

git tag -l -n1 | sort -h | tail -n5
head CHANGES.rst

docker-compose ps
docker-compose up -d
poetry run pytest -vvx --tb=short tests
#| 1130 passed, 31 skipped, 5 warnings in 293.94s (0:04:53)
docker-compose down

version=0.1.47
xdg-open https://github.com/atviriduomenys/spinta/compare/$version..master
xdg-open https://github.com/atviriduomenys/spinta/compare/$version...master
# Update CHANGES.rst
poetry run rst2html.py CHANGES.rst var/changes.html
xdg-open var/changes.html

version=0.1.48
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
git commit -a -m "Releasing version $version"
git push origin master
git tag -a $version -m "Releasing version $version"
git push origin $version

version=0.1.49
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
