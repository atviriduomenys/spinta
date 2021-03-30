import csv
import re
from io import StringIO
from urllib.parse import parse_qsl
from urllib.parse import urlparse

import requests

gsheet_key_re = re.compile('/d/([^/]+)/')


def read_gsheets_manifest(path: str):
    purl = urlparse(path)

    if match := gsheet_key_re.search(purl.path):
        key = match.group(1)
    else:
        raise RuntimeError(f"Unknown Google Sheet URL {path!r}.")

    url = f'https://docs.google.com/spreadsheets/d/{key}/gviz/tq'

    gid = None
    if purl.query:
        gid = dict(parse_qsl(purl.query)).get('gid')
    if gid is None and purl.fragment:
        gid = dict(parse_qsl(purl.fragment)).get('gid')

    params = {
        'tqx': 'out:csv',
        'gid': gid,
    }

    resp = requests.get(url, params)
    resp.raise_for_status()
    yield from csv.reader(StringIO(resp.text))

