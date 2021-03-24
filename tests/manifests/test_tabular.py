from spinta.testing.tabular import create_tabular_manifest
from spinta.testing.tabular import load_tabular_manifest


def check(tmpdir, rc, table):
    create_tabular_manifest(tmpdir / 'manifest.csv', table)
    manifest = load_tabular_manifest(rc, tmpdir / 'manifest.csv')
    assert manifest == table


def test_loading(tmpdir, rc):
    check(tmpdir, rc, '''
    id | d | r | b | m | property | source      | prepare   | type       | ref     | level | access | uri | title   | description
       | datasets/gov/example     |             |           |            |         |       | open   |     | Example |
       |   | data                 |             |           | postgresql | default |       | open   |     | Data    |
       |                          |             |           |            |         |       |        |     |         |
       |   |   |   | country      |             | code='lt' |            | code    |       | open   |     | Country |
       |   |   |   |   | code     | kodas       | lower()   | string     |         | 3     | open   |     | Code    |
       |   |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
       |                          |             |           |            |         |       |        |     |         |
       |   |   |   | city         |             |           |            | name    |       | open   |     | City    |
       |   |   |   |   | name     | pavadinimas |           | string     |         | 3     | open   |     | Name    |
       |   |   |   |   | country  | šalis       |           | ref        | country | 4     | open   |     | Country |
    ''')


def test_uri(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property | type       | ref     | uri
                             | prefix     | locn    | http://www.w3.org/ns/locn#
                             |            | ogc     | http://www.opengis.net/rdf#
    datasets/gov/example     |            |         |
      | data                 | postgresql | default |
                             |            |         |
      |   |   | country      |            | code    |
      |   |   |   | code     | string     |         |
      |   |   |   | name     | string     |         | locn:geographicName
                             |            |         |
      |   |   | city         |            | name    |
      |   |   |   | name     | string     |         | locn:geographicName
      |   |   |   | country  | ref        | country |
    ''')


def test_backends(tmpdir, rc):
    check(tmpdir, rc, f'''
    d | r | b | m | property | type | ref | source
      | default              | sql  |     | sqlite:///{tmpdir}/db
    ''')


def test_backends_with_models(tmpdir, rc):
    check(tmpdir, rc, f'''
    d | r | b | m | property | type   | ref | source
      | default              | sql    |     | sqlite:///{tmpdir}/db
                             |        |     |
      |   |   | country      |        |     | code
      |   |   |   | code     | string |     |
      |   |   |   | name     | string |     |
    ''')


def test_ns(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property | type | ref                  | title               | description
                             | ns   | datasets             | All datasets        | All external datasets.
                             |      | datasets/gov         | Government datasets | All government datasets.
                             |      | datasets/gov/example | Example             |
    ''')


def test_ns_with_models(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property | type   | ref                  | title               | description
                             | ns     | datasets             | All datasets        | All external datasets.
                             |        | datasets/gov         | Government datasets | All government datasets.
                             |        | datasets/gov/example | Example             |
    datasets/gov/example     |        |                      |                     |
      | data                 |        | default              |                     |
                             |        |                      |                     |
      |   |   | Country      |        |                      |                     |
      |   |   |   | name     | string |                      |                     |
    ''')


def test_enum(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property     | type   | source | prepare | access  | title | description
    datasets/gov/example         |        |        |         |         |       |
      | data                     |        |        |         |         |       |
                                 |        |        |         |         |       |
      |   |   | Country          |        |        |         |         |       |
      |   |   |   | name         | string |        |         |         |       |
      |   |   |   | driving_side | string |        |         |         |       |
                                 | enum   | l      | 'left'  | open    | Left  | Left side.
                                 |        | r      | 'right' | private | Right | Right side.
    ''')


def test_lang(tmpdir, rc):
    check(tmpdir, rc, '''
    d | r | b | m | property | type   | ref     | prepare | title       | description
    datasets/gov/example     |        |         |         | Example     | Example dataset.
                             | lang   | lt      |         | Pavyzdys    | Pavyzdinis duomenų rinkinys.
      | data                 |        | default |         |             |
                             |        |         |         |             |
      |   |   | Country      |        |         |         | Country     | Country data model.
                             | lang   | lt      |         | Šalis       | Šalies duomenų modelis.
      |   |   |   | name     | string |         |         | Name        | Country name.
                             | lang   | lt      |         | Pavadinimas | Šalies pavadinimas.
      |   |   |   | driving  | string |         |         | Driving     | Driving side.
                             | lang   | lt      |         | Vairavimas  | Eismo pusė kelyje.
                             | enum   |         | 'left'  | Left        | Left side.
                             | lang   | lt      |         | Kairė       | Kairė pusė.
                             | enum   |         | 'right' | Right       | Right side.
                             | lang   | lt      |         | Dešinė      | Dešinė pusė.
    ''')
