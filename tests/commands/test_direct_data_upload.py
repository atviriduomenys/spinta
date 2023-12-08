import csv
import os

from pathlib import Path
from spinta.core.config import RawConfig
from spinta.components import Mode
from spinta.testing.client import create_test_client
from spinta.testing.manifest import prepare_manifest


def test_direct_data_upload_csv_via_form(
    rc: RawConfig,
    tmp_path: Path,
):
    file_path = f'{tmp_path}/data.csv'
    if os.path.exists(file_path):
        os.remove(file_path)
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)

        writer.writerow(["id", "name", "country"])
        writer.writerow([0, "Vilnius", "Lietuva"])
        writer.writerow([1, "Ryga", "Latvija"])
        writer.writerow([2, "Kaunas", "Lietuva"])
        writer.writerow([3, "Berlynas", "Vokietija"])

    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | ref  | source   | prepare | access
    example/direct/data      |         |      |          |         |
      | csv                  | csv     |      |          |         |
      |   |   | City         |         | name |          |         |
      |   |   |   | name     | string  |      | miestas  |         | open
      |   |   |   | country  | string  |      | šalis    |         | open
    ''', mode=Mode.external)
    context.loaded = True

    app = create_test_client(context)
    app.authmodel('example/direct/data/City', ['getall', 'insert', 'post'])
    headers = {'Content-type': 'text/csv'}

    with open(file_path, "r+b") as file:
        response = app.post('/example/direct/data/City',
                            headers=headers,
                            files={'file': ('data.csv', file, 'text/csv')})

    assert response.status_code == 200


def test_direct_data_upload_csv_via_body(
    rc: RawConfig,
    tmp_path: Path,
):
    context, manifest = prepare_manifest(rc, '''
    d | r | b | m | property | type    | ref  | source   | prepare | access
    example/direct/data      |         |      |          |         |
      | csv                  | csv     |      |          |         |
      |   |   | City         |         | name |          |         |
      |   |   |   | name     | string  |      | miestas  |         | open
      |   |   |   | country  | string  |      | šalis    |         | open
    ''', mode=Mode.external)
    context.loaded = True

    app = create_test_client(context)
    app.authmodel('example/direct/data/City', ['insert'])

    data = ('id, name, country\n'
            '0, Vilnius, Lietuva\n'
            '1, Kaunas, Lietuva\n'
            '2, Ryga, Latvija\n'
            '3, Berlynas, Vokietija'
            )
    headers = {'Content-type': 'text/csv'}
    response = app.post('/example/direct/data/City', content=data, headers=headers)
    assert response.status_code == 200
