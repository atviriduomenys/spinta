import sys

import snoop
import pprint
import pprintpp

# See: https://github.com/alexmojaki/snoop
snoop.install(
    # Force colors, since pytest captures all output by default.
    color=True,
    # Some tests mock sys.stderr, to we need to pass it directly.
    out=sys.stderr,
)

# User pprintpp for nicer and more readable output.
# https://github.com/alexmojaki/snoop/issues/13
pprint.pformat = pprintpp.pformat

pytest_plugins = ['spinta.testing.pytest']

import asyncio
import pytest  # noqa
import sqlalchemy as sa  # noqa
import sqlalchemy_utils as su  # noqa


@pytest.fixture(autouse=True, scope='function')
def check_if_database_is_clean(rc):
    yield
    dsn = rc.get('backends', 'default', 'dsn', default=None)
    if dsn is not None and su.database_exists(dsn):
        engine = sa.create_engine(dsn)
        schema = sa.MetaData(engine)
        table = sa.Table('_schema', schema, autoload=True)
        query = sa.select([sa.func.count()]).select_from(table)
        with engine.begin() as conn:
            assert conn.execute(query).scalar() == 0


@pytest.fixture(autouse=True, scope='function')
def check_event_loop(rc):
    yield
    asyncio.get_event_loop()
