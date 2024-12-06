.. default-role:: literal

.. _writing-tests:

#############
Writing tests
#############

Spinta uses pytest_ test framework.

You can run all tests like this::

    poetry run pytest -vvx --tb=short tests

If you want to run only one test function, then you do it like this::

    poetry run pytest -vvx --tb=short tests/path/to/file.py::test_func_name

Similarly, you can run whole test file::

    poetry run pytest -vvx --tb=short tests/path/to/file.py

Or even a module::

    poetry run pytest -vvx --tb=short tests/path/to

You can turn on `live logs`__ like this::

    poetry run pytest -vvx --tb=short -o log_cli=true --log-cli-level=DEBUG tests

__ https://docs.pytest.org/en/7.1.x/how-to/logging.html#live-logs

For possible logging levels `see the docs`__.

__ https://docs.python.org/3/library/logging.html#levels


Testing commands
****************

When writing tests, most of the time, you should write tests individual
commands, on agreed contract.

Communication between commands should always happen on agreed and documented
contract. So you don't need to tests whole workflow, just inputs and outputs of
each command.

Of course, there should be few functional tests, that tests whole workflow and
all interactions between commands, but since functional tests are slow, you
should add then when really needed.


Test fixtures
*************

All Spinta specific test fixtures can be found in `spinta.testing.pytest`.

Configuration (rc)
==================

`rc` is a session scope fixture and is an instance of
`spinta.core.config.RawConfig` class.

This fixture provides a base configuration for tests. You can modify
configuration as you need.

Usually `rc` fixture is used together with one of manifest initialization
functions. Following functions are available (from `spinta.testing.manifest`
module):

| `context           = load_manifest_get_context(rc, manifest)`
| `manifest          = load_manifest            (rc, manifest)`
| `context, manifest = load_manifest_and_context(rc, manifest)`

    All these functions loads and links given manifest returning `context`,
    `manifest` or both instances. They do exactly same thing, just return
    different instances.

    These functions will load manifest into memory, bet will not prepare
    backends used by manifest.

    Most of the time, you will use one of these functions, because they are
    fast.

    Manifest can be provided in ASCII format, which will be parsed and
    loaded directly, without accessing any files. ASCII manifest format looks
    like this::


        d | r | b | m | property | type    | ref  | access
        example                  |         |      |
          | data                 | geojson |      |
                                 |         |      |
          |   |   | City         |         | name |
          |   |   |   | name     | string  |      | open

    You can only provide columns, that are need for tests and omit others.

    When you get `manifest` instance, you can compare it with ASCII manifest
    string, `manifest` instance will automatically be converted into string.

    Example:

    .. code-block:: python

        def test_geojson(rc: RawConfig):
            schema = '''
            d | r | b | m | property | type    | ref  | source                        | access
            example                  |         |      |                               |
              | data                 | geojson |      | https://example.com/data.json |
                                     |         |      |                               |
              |   |   | City         |         | name | CITY                          |
              |   |   |   | name     | string  |      | NAME                          | open
            '''
            manifest = load_manifest(rc, schema, mode=Mode.external)
            backend = manifest.models['example/City'].backend
            assert backend.type == 'geojson'
            assert manifest == table


`context, manifest = prepare_manifest(rc, manifest)`

    Same as above, but additionally `prepare` command will be called. In
    addition to loading manifest into memory, backends will also be prepared.
    Backends will be loaded into memory, but actual backend databases will not
    be touched.

    You will need this, when writing tests, that test backends.

`context = bootstrap_manifest(rc, manifest)`

    Same as above, but additionally `bootstrap` command will be called.
    `bootstrap` will initialize backends, creating table if needed other
    backend specific things.

    You will need this, when writing functional tests and when you want to test
    things on a real running backend instance.

    Usually this fixture should be used in combination with a backend fixture:

    - `sqlite` - Sqlite database stored in a file (functions scope).

    - `postgresql` - PostgreSQL with PostGiS extension (session scope).

    - `mongo` - Mongo (session scope).

    Keep in mind, that if backend is session scope, then all tests will reuse
    same database. It is possible to remove all data from tables by passing
    `request` to `bootstrap_manifest` function, but tables will not be dropped.
    So it is best, to run tests on a table with different name for each test.

    Usually `bootstrap_manifest` function is used together with
    `create_test_client`, which initializes an HTTP test client and allows you
    to do HTTP requests.

    These tests are slow, and should be used rarely.
    
    Example:

    .. code-block:: python

        def test_uri(
            rc: RawConfig,
            postgresql: str,
            request: FixtureRequest,
        ):
            context = bootstrap_manifest(rc, '''
            d | r | b | m | property          | type   | ref
            backends/postgres/dtypes/uri      |        |
              |   |   | City                  |        |
              |   |   |   | name              | string |
              |   |   |   | website           | uri    |
            ''', backend=postgresql, request=request)

            app = create_test_client(context)
            app.authmodel('backends/postgres/dtypes/uri/City', [
                'insert',
                'getall',
            ])

            resp = app.get('/backends/postgres/dtypes/uri/City')

    There is an `app` fixture, which is not recommended to use anymore, instead
    `app`, `bootstrap_manifest` function should be used. `app` fixture tries to
    load predefined manifests from file system, but that does not work, because
    each test might want to have a slightly different manifest, so each tests
    should define manifests they need.


.. _pytest: https://pytest.org/
