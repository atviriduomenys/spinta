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
        'paths': {}
    })

    table = '''
        id | d | r | b | m | property | type | ref | source | prepare | level | access | uri | title             | description
           | example_api              |      |     |        |         |       |        |     | Example of an API | Intricate description
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table
