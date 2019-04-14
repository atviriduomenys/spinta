from spinta.dispatcher import command


@command()
def error():
    pass


@command()
def load():
    """Load primitive data structures to python-native objects.

    Currently used for:

    - Load things from configuration:

        load(Context, Config, dict) -> Config
        load(Context, Store, Config) -> Store
        load(Context, Backend, BackendConfig) -> Backend
        load(Context, Manifest, dict) -> Manifest

    - Load nodes from manifest:

        load(Context, Node, dict, Manifest) -> Node

    - Load commands from manifest:

        load(Context, Command, dict, *, scope=None) -> Command

    - Load pimitive data types to python-native objects:

        load(Context, X, Node, Backend) -> Y

    """


@command()
def dump():
    """Dump python-native objects to primitive data structures."""


@command()
def check():
    """Check if input value is correct."""


@command()
def prepare():
    """Prepare value."""


@command()
def migrate():
    """Migrate database schema changes."""


@command()
def push():
    """Insert, update or delete data to the databse."""


@command()
def get():
    """Get single record from the databse."""


@command()
def getall():
    """Find multiple records in the databse."""


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
