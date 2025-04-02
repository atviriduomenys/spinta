import json
from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_open_api_manifest(rc: RawConfig, tmp_path: Path):
    # TODO: Update everytime a new part is supported
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        },
        'tags': [
            {
                'name': 'List of Countries',
                'description': 'A list of world countries'
            }
        ],
        'paths': {
            '/api/countries': {
                'get': {
                    'tags': ['List of Countries']
                }
            },
        }
    })

    table = '''
    id | d | r | b | m | property               | type | ref | source | prepare | level | access | uri | title             | description
       | services                               | ns   |     |        |         |       |        |     |                   |
       | services/example_api                   | ns   |     |        |         |       |        |     | Example of an API | Intricate description
       |                                        |      |     |        |         |       |        |     |                   |
       | services/example_api/list_of_countries |      |     |        |         |       |        |     | List of Countries | A list of world countries
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_no_paths(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        }
    })

    table = '''
    id | d | r | b | m | property               | type | ref | source | prepare | level | access | uri | title             | description
       | services                               | ns   |     |        |         |       |        |     |                   |
       | services/example_api                   | ns   |     |        |         |       |        |     | Example of an API | Intricate description
       |                                        |      |     |        |         |       |        |     |                   |
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table


def test_open_api_manifest_title_with_slashes(rc: RawConfig, tmp_path: Path):
    data = json.dumps({
        'openapi': '3.0.0',
        'info': {
            'title': 'Example API/Example for example',
            'version': '1.0.0',
            'summary': 'Example of an API',
            'description': 'Intricate description'
        }
    })

    table = '''
        id | d | r | b | m | property                 | type | ref | source | prepare | level | access | uri | title             | description
           | services                                 | ns   |     |        |         |       |        |     |                   |
           | services/example_api_example_for_example | ns   |     |        |         |       |        |     | Example of an API | Intricate description
           |                                          |      |     |        |         |       |        |     |                   |
        '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table
