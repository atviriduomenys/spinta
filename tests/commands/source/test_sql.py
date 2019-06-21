import collections

import pytest
import sqlalchemy as sa

from spinta.utils.itertools import consume

SQL = collections.namedtuple('SQL', ('engine', 'schema'))


@pytest.fixture
def sql(config):
    dsn = config.get('backends', 'default', 'dsn', required=True)
    engine = sa.create_engine(dsn)
    schema = sa.MetaData(engine)
    yield SQL(engine, schema)
    schema.drop_all()


def test_sql(sql, context):
    context.load({
        'datasets': {
            'default': {
                'sql': {
                    'db': 'postgresql://admin:admin123@localhost:54321/spinta_tests',
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

    assert consume(context.pull('sql')) == 3
    assert consume(context.pull('sql')) == 0

    assert sorted([(x['code'], x['title']) for x in context.getall('country')]) == []
    assert sorted([(x['code'], x['title']) for x in context.getall('country', dataset='sql', resource='db')]) == [
        ('ee', 'Estija'),
        ('lt', 'Lietuva'),
        ('lv', 'Latvija'),
    ]
