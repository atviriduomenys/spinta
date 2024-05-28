export INSTANCE=manifests/xsd
export BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR
mkdir -p $BASEDIR/schemas

poetry install

# Download XSD files from RC for testing
poetry run python notes/manifests/xsd.py download-rc-broker-xsd-files $BASEDIR/schemas


poetry run spinta copy $BASEDIR/schemas/*.xsd -o $BASEDIR/manifest.csv
# FIXME: I get a lot of debug output, that should be cleaned.

wc -l $BASEDIR/manifest.csv
#| 67,060 var/instances/manifests/xsd/manifest.csv

NUM=67060

# Number of hours
qalc "$NUM * 5 / 60"
#| (67060 × 5) / 60 = 5588 + 1/3 ≈ 5588,333333

# Number of days
qalc "$NUM * 5 / 60 / 8"
#| ((67060 × 5) / 60) / 8 = 698 + 13/24 ≈ 698,5416667

# Number of years
qalc "$NUM * 5 / 60 / 8 / 5 / 4 / 8"

# Number of €
qalc "$NUM * 5 / 60 * 60"
#| ((67060 × 5) / 60) × 60 = 335.300

poetry run spinta show $BASEDIR/manifest.csv

# Extract all elements and attributes from xsd files
poetry run python notes/manifests/xsd.py extract-xpaths-from-xsd-files $BASEDIR/schemas/*.xsd > $BASEDIR/elements.txt

wc -l $BASEDIR/elements.txt
#| 37,689 var/instances/manifests/xsd/elements.txt



