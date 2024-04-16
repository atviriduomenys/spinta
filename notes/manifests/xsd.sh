export INSTANCE=manifests/xsd
export BASEDIR=var/instances/$INSTANCE
mkdir -p $BASEDIR
mkdir -p $BASEDIR/schemas

poetry install

# Download XSD files from RC for testing
poetry run python
import os
import requests
import pathlib
from urllib.parse import urlparse
from urllib.parse import parse_qsl
from urllib.parse import urljoin
from lxml import html

BASE_DIR = pathlib.Path(os.environ['BASEDIR'])
BASE_URL = "https://ws.registrucentras.lt"

def get_query_value(url: str, key: str) -> str:
    urlp = urlparse(url)
    query = dict(parse_qsl(urlp.query))
    if key not in query:
        raise KeyError(f"URL query param {key!r} is not in URL {url!r}.")
    return query[key]


session = requests.Session()
response = session.get(urljoin(BASE_URL, '/broker/info.php'))
document_list = html.fromstring(response.text)
urls = document_list.xpath("//*[contains(@href, 'out')]")
for url in urls:
    url = url.attrib["href"]
    file_number = get_query_value(url, 't')
    file_name = BASE_DIR / f'schemas/out_{file_number}.xsd'
    response = session.get(urljoin(BASE_URL, url))
    with file_name.open("w") as file:
        file.write(response.text)
    print(f'{file_name} <- {url}')


jar_url = urljoin(BASE_URL, "/broker/xsd.klasif.php?kla_grupe=JAR")
ntr_url = urljoin(BASE_URL, "/broker/xsd.klasif.php?kla_grupe=NTR")
klasif_urls = [
    ('jar', jar_url),
    ('ntr', ntr_url),
]
for prefix, klasif_url in klasif_urls:
    response = session.get(klasif_url)
    document_list = html.fromstring(response.text)
    urls = document_list.xpath("//*[contains(@href, 'kla_kodas')]")
    for url in urls:
        url = url.attrib["href"]
        file_number = get_query_value(url, 'kla_kodas')
        file_name = BASE_DIR / f'schemas/rc_{prefix}_klasif_{file_number}.xsd'
        response = session.get(urljoin(BASE_URL, url))
        with file_name.open("w") as file:
            file.write(response.text)
        print(f'{file_name} <- {url}')


urls = [
    "/broker/xsd.jadis.php?f=jadis-israsas.xsd",
    "/broker/xsd.jadis.php?f=jadis-sarasas.xsd",
    "/broker/xsd.jadis.php?f=jadis-dalyvio-israsas.xsd",
]
for url in urls:
    file_name = BASE_DIR / get_query_value(url, 'f')
    response = requests.get(urljoin(BASE_URL, url))
    with file_name.open("w") as file:
        file.write(response.text)
        print(f'{file_name} <- {url}')

exit()

poetry run spinta copy $BASEDIR/schemas/*.xsd -o $BASEDIR/manifest.csv
# FIXME: I get a lot of debug output, that should be cleaned.

wc -l $BASEDIR/manifest.csv
#| 67,060 var/instances/manifests/xsd/manifest.csv

poetry run spinta show $BASEDIR/manifest.csv
