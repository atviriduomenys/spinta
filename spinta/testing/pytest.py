import os
import pathlib
import tempfile
from difflib import Differ
from typing import Any, Iterator

import pprintpp
import pytest
import sqlalchemy as sa
import sqlalchemy_utils as su
from sqlalchemy.engine.url import make_url, URL
from responses import RequestsMock
from sqlalchemy import text
from sqlalchemy.pool import NullPool

from spinta.core.config import RawConfig
from spinta.core.config import read_config
from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap
from spinta.manifests.components import Manifest
from spinta.testing.cli import SpintaCliRunner
from spinta.testing.client import TestClient
from spinta.testing.client import create_test_client
from spinta.testing.config import CONFIG
from spinta.testing.context import ContextForTests
from spinta.testing.context import create_test_context
from spinta.testing.datasets import Sqlite
from spinta.testing.manifest import compare_manifest


def _remove_push_state(rc: RawConfig) -> None:
    data_dir = rc.get("data_path")
    for file in (data_dir / "push").glob("*.db"):
        file.unlink()


@pytest.fixture(scope="session")
def rc():
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = pathlib.Path(tmpdir)
        data_dir = tmpdir / ".local/share"
        rc = read_config()
        rc.add(
            "pytest",
            {
                "env": "test",
                "data_path": data_dir,
                "keymaps.default": {
                    "type": "sqlalchemy",
                    "dsn": "sqlite:///{data_dir}/keymap.db",
                },
            },
        )
        rc.lock()
        yield rc
        _remove_push_state(rc)


@pytest.fixture()
def sqlite():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Sqlite("sqlite:///" + os.path.join(tmpdir, "db.sqlite"))


def _prepare_postgresql(dsn: str) -> None:
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder"))


def _terminate_backends_same_server(target_db_url: str) -> None:
    """
    Terminate all sessions connected to the DB indicated by target_db_url,
    using a connection to a *different* DB on the same server ('postgres' or 'template1').
    """
    u = make_url(target_db_url)
    target_dbname = u.database

    last_err = None
    for admin_db in ("postgres", "template1"):
        admin_url = str(u.set(database=admin_db))
        try:
            eng = sa.create_engine(admin_url, isolation_level="AUTOCOMMIT", poolclass=NullPool)
            try:
                with eng.connect() as c:
                    # block new connections first
                    c.execute(text(f'ALTER DATABASE "{target_dbname}" WITH ALLOW_CONNECTIONS = false'))
                    # kill everyone except ourselves
                    c.execute(
                        text("""
                            SELECT pg_terminate_backend(pid)
                            FROM pg_stat_activity
                            WHERE datname = :db AND pid <> pg_backend_pid()
                        """),
                        {"db": target_dbname},
                    )
            finally:
                eng.dispose()
            return  # success
        except sa.exc.SQLAlchemyError as e:
            last_err = e
            continue
    # If both admin DBs failed to connect/operate, surface the last error
    if last_err:
        raise last_err


@pytest.fixture(scope="session")
def postgresql(rc) -> str:
    """
    Session-wide base DSN fixture. Monkeypatch su.drop_database so any drop
    first terminates lingering connections from a *different* DB.
    """
    dsn: str = rc.get("backends", "default", "dsn", required=True)

    # Wrap sqlalchemy_utils.drop_database globally for this session.
    original_drop = su.drop_database

    def drop_force(db_url: str):
        # ensure we are NOT connected to the target DB when altering it
        _terminate_backends_same_server(db_url)
        return original_drop(db_url)

    su.drop_database = drop_force  # monkeypatch for the session

    try:
        if not su.database_exists(dsn):
            su.create_database(dsn)
        _prepare_postgresql(dsn)
        yield dsn
    finally:
        # restore original
        su.drop_database = original_drop
        # (optional) if you previously dropped the base DSN at teardown, keep doing so safely
        if su.database_exists(dsn):
            _terminate_backends_same_server(dsn)
            original_drop(dsn)


@pytest.fixture(scope="session")
def mongo(rc):
    yield
    dsn = rc.get("backends", "mongo", "dsn", required=False)
    db = rc.get("backends", "mongo", "db", required=False)
    if dsn and db:
        import pymongo

        client = pymongo.MongoClient(dsn)
        client.drop_database(db)


@pytest.fixture(scope="session")
def backends(postgresql, mongo):
    yield {
        "postgresql": postgresql,
        "mongo": mongo,
    }


@pytest.fixture(scope="session")
def _context(rc: RawConfig, postgresql, mongo):
    context: ContextForTests = create_test_context(rc)
    context.load()
    yield context


@pytest.fixture
def context(_context, mocker, tmp_path, request):
    with _context.fork("test") as context:
        store = context.get("store")
        if "fs" in store.backends:
            # XXX: There must be a better way to provide tmpdir to fs backend.
            mocker.patch.object(store.backends["fs"], "path", tmp_path)

        # In-memory accesslog used with spinta.accesslog.python.
        context.set("accesslog.stream", [])

        yield context

        # At this point, transaction must be closed, if it is not, then something is
        # wrong. Find out why transaction was not property closed.
        assert context.has("transaction") is False

        # If context was not loaded, then it means, that database was not touched.
        # All database operations require fully loaded context.
        if context.loaded:
            # XXX: Maybe instead of deleting everything, we could rollback
            #      transactions, once this kind of functionality will be
            #      available? This should be more efficient.
            context.wipe_all()


@pytest.fixture
def responses() -> Iterator[RequestsMock]:
    with RequestsMock() as mock:
        yield mock


@pytest.fixture
def app(context) -> TestClient:
    context.attach("client", create_test_client, context)
    return context.get("client")


@pytest.fixture
def cli(rc: RawConfig):
    yield SpintaCliRunner(mix_stderr=False)
    _remove_push_state(rc)


@pytest.fixture(scope="session", autouse=True)
def setup_filesystem():
    backends = CONFIG["environments"]["test"]["backends"]
    if "fs" in backends:
        path = backends["fs"]["path"]
        if isinstance(path, pathlib.Path):
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)


def pytest_addoption(parser):
    # TODO: Switch back to backend pytest param.
    #       We want to tests multiple backends and one backend can use
    #       multiple models.
    parser.addoption(
        "--model",
        action="append",
        default=[],
        help="run tests only for particular model ['postgres', 'mongo', 'postgres/datasets']",
    )
    parser.addoption(
        "--manifest_type",
        action="append",
        default=[],
        help="run tests only for particular manifest ['internal_sql', 'csv', 'ascii']",
    )


def pytest_configure(config):
    # https://docs.pytest.org/en/latest/mark.html#registering-marks
    config.addinivalue_line("markers", "models(*models): mark test to run multiple times with each model specified")
    config.addinivalue_line(
        "markers", "manifests(*manifests): mark test to run multiple times with each manifest type specified"
    )


def pytest_generate_tests(metafunc):
    # Get model markers from test, if markers are set - leave test as is
    models = metafunc.definition.get_closest_marker("models")
    if models:
        # If there are markers, get them, together with model CLI options
        models = set(models.args)
        model_cli_options = set(metafunc.config.getoption("model"))

        # If model CLI options are not empty
        # then get common markers from test and CLI options
        if model_cli_options:
            models = models.intersection(model_cli_options)

        # Parametrize our test with calculated models.
        # If we pass to CLI model option, which does not have a test marker,
        # then pytest will skip the test all together.
        metafunc.parametrize("model", models)

    manifests = metafunc.definition.get_closest_marker("manifests")
    if manifests:
        # If there are markers, get them, together with manifest CLI options
        manifests = set(manifests.args)
        manifest_cli_options = set(metafunc.config.getoption("manifest_type"))

        # If model CLI options are not empty
        # then get common markers from test and CLI options
        if manifest_cli_options:
            manifests = manifests.intersection(manifest_cli_options)

        # Parametrize our test with calculated manifests.
        # If we pass to CLI model option, which does not have a test marker,
        # then pytest will skip the test all together.
        metafunc.parametrize("manifest_type", manifests)


def _diff_line(line: str) -> str:
    if line.startswith("- "):
        return "< " + line[2:]
    if line.startswith("+ "):
        return "> " + line[2:]
    return line


@pytest.hookimpl(tryfirst=True)
def pytest_assertrepr_compare(op: str, left: Any, right: Any):
    if op == "==" and isinstance(left, Manifest) and isinstance(right, str):
        left, right = compare_manifest(left, right)
        return ["not equal"] + [
            _diff_line(line)
            for line in Differ().compare(
                left.splitlines(),
                right.splitlines(),
            )
        ]
    types = (dict, list)
    if op == "==" and isinstance(left, types) and isinstance(right, types):
        left = pprintpp.pformat(left, indent=2, width=40).splitlines()
        right = pprintpp.pformat(right, indent=2, width=40).splitlines()
        return ["not equal"] + [_diff_line(line) for line in Differ().compare(left, right)]


MIGRATION_DATABASE = "spinta_tests_migration"


@pytest.fixture(scope="module")
def postgresql_migration(rc) -> URL:
    url = make_url(rc.get("backends", "default", "dsn", required=True))
    url = url.set(database=MIGRATION_DATABASE)

    if su.database_exists(url):
        _prepare_migration_postgresql(url)
        yield url
    else:
        su.create_database(url)
        _prepare_migration_postgresql(url)
        yield url
        su.drop_database(url)


@pytest.fixture(scope="function")
def reset_keymap(context):
    def _reset_keymap(excluded_tables: list[str] = None):
        keymap.metadata.reflect()
        with keymap.engine.connect() as conn:
            for key, table in keymap.metadata.tables.items():
                if excluded_tables and key in excluded_tables:
                    continue
                conn.execute(table.delete())

    keymap = context.get("store").keymaps["default"]
    excluded = []
    if isinstance(keymap, SqlAlchemyKeyMap):
        excluded.append(keymap.migration_table_name)
    _reset_keymap(excluded)
    yield
    _reset_keymap(excluded)


def _prepare_migration_postgresql(dsn: URL) -> None:
    engine = sa.create_engine(dsn)
    with engine.connect() as conn:
        conn.execute(sa.text("DROP SCHEMA public CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA public"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS btree_gist"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch"))
        conn.execute(sa.text("CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder"))
