from spinta.dispatcher import command


@command()
def error():
    pass


@command()
def load():
    """Load primitive data structures to python-native objects.

    Currently used for:

    - Load things from configuration:

        load(Context, Config, RawConfig) -> Config
        load(Context, Store, Config) -> Store
        load(Context, Backend, Config) -> Backend
        load(Context, Manifest, dict) -> Manifest

    - Load nodes from manifest:

        load(Context, Node, dict, Manifest) -> Node

    - Load commands from manifest:

        load(Context, Command, dict, *, scope=None) -> Command

    - Load primitive data types to python-native objects:

        load(Context, X, Node, Backend) -> Y

    - Load data from request:

        load(Context, Property, Request)

    """


@command()
def load_search_params():
    """Load search parameters as native python values.
    """


@command()
def load_operator_value():
    """Loads URL query operator value given by the user.
    """


@command()
def wait():
    """Wait while all database backends are up.

    Database backends are a separate services and once you start multiple
    services at the same time, you need to make sure, that all the required
    external services are up before running the app.
    """


@command()
def dump():
    """Dump python-native objects to primitive data structures.


    - Dump backend-native values to primitive data structures.

        dump(Context, Backend, Type, object)

    """


@command()
def check():
    """Check if input value is correct.

    - Check components after load:

        check(Context, Config):
        check(Context, Store):
        check(Context, Manifest):
        check(Context, Node):

    - Check data before push:

        check(Context, Model, Backend, dict, *, action: Action)
        check(Context, Type, Property, Backend, object, *, data: dict, action: Action)

    """


@command()
def prepare():
    """Prepare value.

    - Prepare database backend:

        prepare(Context, Store)
        prepare(Context, Manifest)
        prepare(Context, Backend, Manifest)
        prepare(Context, Backend, Node)
        prepare(Context, Backend, Type)

      Here, sqlalchemy.MetaData object is populated with tables and columns.

    - Build UrlParams from Request:

        prepare(Context, UrlParams, Version, Request) -> UrlParams

    - Convert Python-native values backend-native values:

        prepare(Context, Model, dict) -> dict
        prepare(Context, Property, Backend, object) -> object
        prepare(Context, Type, Backend, object) -> object

    - Convert backend-native values to Python-native values:

        # FIXME: probably this should be replaced with `dump`.
        prepare(Context, Action, Model, Backend, object) -> dict

    - Prepare external dataset for data import:

        prepare(Context, Source, Node)

    """


@command()
def migrate():
    """Migrate database schema changes."""


@command()
def authorize():
    """Check if user is authorized to access a resource."""


@command()
def push():
    """Insert, update or delete data to the databse."""


@command()
def insert():
    pass


@command()
def upsert():
    pass


@command()
def update():
    pass


@command()
def patch():
    pass


@command()
def delete():
    pass


@command()
def getone():
    """Get single record from the databse."""


@command()
def getall():
    """Find multiple records in the databse."""


@command()
def get_version():
    """Returns version dict for the api"""


@command()
def changes():
    """Changelog of a table."""


@command()
def pull():
    """Pull data from external data sources."""


@command()
def export():
    """Export data in a specified format."""


@command()
def wipe():
    """Delete all data from specified model."""


@command()
def gen_object_id():
    """Genearet unique id.

    gen_object_id(Context, Backend, Model) -> str

    """


@command()
def is_object_id():
    """Check if given string is object id.

    - Detect if given string is an object id:

        is_object_id(Context, str) -> bool

    - Detect if given string is an object id of given backend and model.

        is_object_id(Context, Backend, Model, str) -> bool

    """


@command()
def contents():
    """Model namespace contents.

    For example if we have two models named `foo/bar/baz` ant `foo/bar/bac`,
    then if client requests `/foo/bar` they will get `[baz, bac]`.

    """


@command()
def render():
    """Render response to the clinet."""
