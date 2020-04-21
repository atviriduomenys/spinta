import sqlalchemy as sa

from spinta.testing.utils import create_manifest_files
from spinta.testing.client import create_test_client
from spinta.testing.context import create_test_context


def test_getall(rc, cli, tmpdir, request):
    create_manifest_files(tmpdir / 'manifest', {
        'datasets/example.datset.yml': {
            'type': 'dataset',
            'name': 'datasets/example',
            'resources': {
                'countries': {
                    'type': 'sql',
                    'backend': 'sql',
                },
            },
        },
        'datasets/example/country.yml': {
            'type': 'model',
            'name': 'datasets/example/country',
            'external': {
                'dataset': 'datasets/example',
                'resource': 'countries',
                'name': 'tests_country',
                'pk': 'code',
            },
            'properties': {
                'code': {
                    'type': 'string',
                    'level': 4,
                    'access': 'open',
                    'external': 'kodas',
                },
                'title': {
                    'type': 'string',
                    'level': 4,
                    'access': 'open',
                    'external': 'pavadinimas',
                },
            },
        },
    })

    rc = rc.fork({
        'manifests.default': {
            'type': 'yaml',
            'path': str(tmpdir / 'manifest'),
            'backend': 'default',
        },
        'backends': {
            'default': {
                'type': 'memory',
            },
            'sql': {
                'type': 'sql',
                'dsn': 'sqlite:///' + str(tmpdir / 'db.sqlite'),
            },
        },
    })

    dsn = rc.get('backends', 'sql', 'dsn')
    engine = sa.create_engine(dsn)
    schema = sa.MetaData(engine)
    country = sa.Table(
        'tests_country', schema,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('kodas', sa.Text),
        sa.Column('pavadinimas', sa.Text),
    )
    schema.create_all()

    with engine.begin() as conn:
        conn.execute(country.insert(), [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
        ])

    context = create_test_context(rc, name='pytest/client')
    request.addfinalizer(context.wipe_all)

    app = create_test_client(context)
    app.authmodel('datasets/example/country', ['search_external'])

    resp = app.get('/datasets/example/country/:external?sort(code)')
    rows = [(d['code'], d['title'], d['_type']) for d in resp.json()['_data']]
    assert rows == [
        ('ee', 'Estija', 'datasets/example/country'),
        ('lt', 'Lietuva', 'datasets/example/country'),
        ('lv', 'Latvija', 'datasets/example/country'),
    ]
