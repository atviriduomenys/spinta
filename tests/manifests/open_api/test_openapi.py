import json
from pathlib import Path

from spinta.core.config import RawConfig
from spinta.testing.manifest import load_manifest


def test_open_api_manifest(rc: RawConfig, tmp_path: Path):
    # TODO: Temporary test, will be adjusted once actual logic is implemented.
    data = json.dumps({
            "openapi": "3.0.0",
            "info": {
            "title": "openapi",
            "version": "1.0.0"
        },
        "paths": {}
    })

    table = '''
        id | d | r | b | m | property | type | ref | source | prepare | level | access | uri | title | description
           | openapi                  |      |     |        |         |       |        |     |       |
    '''

    path = tmp_path / 'manifest.json'
    path_openapi = f'openapi+file://{path}'
    with open(path, 'w') as json_file:
        json_file.write(data)

    manifest = load_manifest(rc, path_openapi)

    assert manifest == table
