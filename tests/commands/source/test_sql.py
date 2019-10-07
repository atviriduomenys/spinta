import collections

import pytest
import sqlalchemy as sa

from spinta.testing.context import create_test_context

SQL = collections.namedtuple('SQL', ('engine', 'schema'))


@pytest.fixture
def sql(config):
    dsn = config.get('backends', 'default', 'dsn', required=True)
    engine = sa.create_engine(dsn)
    schema = sa.MetaData(engine)
    yield SQL(engine, schema)
    schema.drop_all()


def test_sql(config, sql, app):
    context = create_test_context(config)
    context.load({
        'datasets': {
            'default': {
                'sql': {
                    'db': config.get('backends', 'default', 'dsn'),
                },
            }
        },
    })

    country = sa.Table(
        'tests_country', sql.schema,
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('kodas', sa.Text),
        sa.Column('pavadinimas', sa.Text),
    )
    sql.schema.create_all()

    with sql.engine.begin() as conn:
        conn.execute(country.insert(), [
            {'kodas': 'lt', 'pavadinimas': 'Lietuva'},
            {'kodas': 'lv', 'pavadinimas': 'Latvija'},
            {'kodas': 'ee', 'pavadinimas': 'Estija'},
        ])

    assert len(context.pull('sql')) == 3
    assert len(context.pull('sql')) == 0

    app.authorize(['spinta_getall', 'spinta_search'])

    assert app.getdata('/country') == []
    assert app.getdata('/country/:dataset/sql?select(code,title)&sort(+code)') == [
        {'code': 'ee', 'title': 'Estija'},
        {'code': 'lt', 'title': 'Lietuva'},
        {'code': 'lv', 'title': 'Latvija'},
    ]
