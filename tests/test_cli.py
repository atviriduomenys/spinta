import pathlib

import pytest

from responses import GET

from spinta.cli import pull, push
from spinta.testing.client import create_remote_server
from spinta.testing.client import create_client_creentials_file
from spinta.testing.utils import create_manifest_files
from spinta.testing.utils import update_manifest_files
from spinta.testing.data import pushdata


@pytest.mark.skip('datasets')
def test_pull(responses, rc, cli, app, tmpdir):
    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, content_type='text/plain; charset=utf-8',
        body=(
            'kodas,šalis\n'
            'lt,Lietuva\n'
            'lv,Latvija\n'
            'ee,Estija'
        ),
    )

    create_manifest_files(tmpdir, {
        'datasets/csv.yml': None,
        'datasets/csv/country.yml': None,
    })
    update_manifest_files(tmpdir, {
        'datasets/csv.yml': [
            {'op': 'add', 'path': '/resources/countries/backend', 'value': 'csv'},
        ],
    })
    rc = rc.fork({
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir),
        },
        'backends.csv': {
            'type': 'csv',
        },
    })

    result = cli.invoke(rc, pull, ['datasets/csv'])
    assert result.output == (
        '\n'
        '\n'
        'Table: country/:dataset/csv/:resource/countries\n'
        '                  _id                      code    title     _op                         _where                    \n'
        '===================================================================================================================\n'
        '552c4c243ec8c98c313255ea9bf16ee286591f8c   lt     Lietuva   upsert   _id="552c4c243ec8c98c313255ea9bf16ee286591f8c"\n'
        'b5dcb86880816fb966cdfbbacd1f3406739464f4   lv     Latvija   upsert   _id="b5dcb86880816fb966cdfbbacd1f3406739464f4"\n'
        '68de1c04d49aeefabb7081a5baf81c055f235be3   ee     Estija    upsert   _id="68de1c04d49aeefabb7081a5baf81c055f235be3"'
    )

    app.authmodel('country/:dataset/csv/:resource/countries', ['getall'])

    assert app.get('/country/:dataset/csv/:resource/countries').json() == {
        '_data': [],
    }

    result = cli.invoke(rc, pull, ['csv', '--push'])
    assert 'csv:' in result.output

    rows = sorted(
        (row['code'], row['title'], row['_type'])
        for row in app.get('/country/:dataset/csv/:resource/countries').json()['_data']
    )
    assert rows == [
        ('ee', 'Estija', 'country/:dataset/csv/:resource/countries'),
        ('lt', 'Lietuva', 'country/:dataset/csv/:resource/countries'),
        ('lv', 'Latvija', 'country/:dataset/csv/:resource/countries'),
    ]
