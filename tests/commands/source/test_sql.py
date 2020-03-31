import collections

import pytest
import sqlalchemy as sa

from spinta.testing.datasets import pull

SQL = collections.namedtuple('SQL', ('engine', 'schema'))


@pytest.fixture
def sql(rc):
    dsn = rc.get('backends', 'default', 'dsn', required=True)
    engine = sa.create_engine(dsn)
    schema = sa.MetaData(engine)
    yield SQL(engine, schema)
    schema.drop_all()


@pytest.mark.skip('datasets')
def test_sql(rc, cli, sql, app):
    dsn = rc.get('backends', 'default', 'dsn', required=True)
    rc = rc.fork({'datasets.default.sql.db': dsn})

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

    assert len(pull(cli, rc, 'sql')) == 3
    assert len(pull(cli, rc, 'sql')) == 0

    app.authorize(['spinta_getall', 'spinta_search'])

    assert app.getdata('/country') == []
    assert app.getdata('/country/:dataset/sql?select(code,title)&sort(+code)') == [
        {'code': 'ee', 'title': 'Estija'},
        {'code': 'lt', 'title': 'Lietuva'},
        {'code': 'lv', 'title': 'Latvija'},
    ]
