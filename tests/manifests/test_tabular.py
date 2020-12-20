import pytest

from spinta.testing.tabular import SHORT_NAMES
from spinta.testing.tabular import striptable
from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest
from spinta.testing.tabular import render_tabular_manifest


def _get_cols(table):
    cols = [c.strip() for c in table.splitlines()[0].split('|')]
    return [SHORT_NAMES.get(c, c) for c in cols]


def check(tmpdir, rc, table):
    table = striptable(table)
    create_tabular_manifest(tmpdir / 'manifest.csv', table)
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    assert render_tabular_manifest(manifest, _get_cols(table)) == table


def test_loading(tmpdir, rc):
    check(tmpdir, rc, '''
    id | d | r | b | m | property | source      | prepare   | type   | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |        |         |       | open   |     | Example |
       |   | data                 |             |           |        | default |       | open   |     | Data    |
       |                          |             |           |        |         |       |        |     |         |
       |   |   |   | country      |             | code='lt' |        | code    |       | open   |     | Country |
       |   |   |   |   | code     | kodas       | lower()   | string |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |                          |             |           |        |         |       |        |     |         |
       |   |   |   | city         |             |           |        | name    |       | open   |     | City    |
       |   |   |   |   | name     | pavadinimas |           | string |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | šalis       |           | ref    | country | 4     | open   |     | Country |
    ''')


def test_uri(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property | type   | ref     | uri
                             | prefix | locn    | http://www.w3.org/ns/locn#
    datasets/gov/example     |        |         |
      | data                 |        | default |
                             |        |         |
      |   |   | country      |        | code    |
      |   |   |   | code     | string |         |
      |   |   |   | name     | string |         | locn:geographicName
                             |        |         |
      |   |   | city         |        | name    |
      |   |   |   | name     | string |         | locn:geographicName
      |   |   |   | country  | ref    | country |
    ''')
