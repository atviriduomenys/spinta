from spinta.dispatcher import command


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
def simple_data_check():
    """Run a simple data check.

    Check data by using restrictions given in schema, do not run any business
    logic. Also simple data check should not do any database queries.

    Simple data check is run early, before retrieving exisisting data from
    database.

    Later a complex data check is run with existing data loaded from database.
    """


@command()
def complex_data_check():
    """Run a complex data check.

    At this point, simple data check is already done and data is passed simple
    validation.

    Complex data check will receive existing data in database, additional
    queries can be run to do more complex validations.
    """


@command()
def check_unique_constraint():
    """Check if value is unique.

    This check is only performed when peroperty has unique property set to true.
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
def insert_many():
    pass


@command()
def upsert():
    pass


@command()
def upsert_many():
    pass


@command()
def update():
    pass


@command()
def update_many():
    pass


@command()
def patch():
    pass


@command()
def patch_many():
    pass


@command()
def delete():
    pass


@command()
def delete_many():
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
def render():
    """Render response to the clinet."""


@command()
def unload_backend():
    """Unload database backend.

    Currently this is only used in tests and only for PostgreSQL, after each
    tests, all sqlalchemy connections are disposed to avoid leaving open
    connections between tests.
    """


@command()
def get_referenced_model():
    """Find referenced model by given property type."""


@command()
def make_json_serializable():
    """Convert given Python-native value to a JSON serializable value."""


@command()
def get_error_context():
    """Get error context for a given object."""


@command()
def in_namespace():
    """Check if `a` is in `b` namespace."""


@command()
def create_changelog_entry():
    """Create new changelog entry."""


@command()
def coerce_source_value():
    """Coerce value received from external data source to python native type."""


@command()
def new_schema_version():
    """Calculate diff between two nodes."""


@command()
def build_data_patch_for_write():
    """Builds data patch dict for backend consumption on write.

    Purpose of this command is to generate a patch, by comparing given and saved
    values. If given and saved values are same, then we exclude this property
    from patch, because nothing has changed.

    Optionally patch can be filled with default values. That means, if given
    value does not have all properties defined in schema, then we fill those
    missing values with defaults and only then proceed with patch generation.

    In the end, this command produces partial patch, with only those values
    which has been changed.

    """


@command()
def build_full_data_patch_for_nested_attrs():
    """Builds full patch from data.patch and data.saved for nested fields

    This solves problems when doing updates on nested fields.

    Usual problems:
    - Some database backends (e.g. Postgres) do not support partial updates
    to nested fields, thus "full patch" must be constructed for nested fields
    from partial `data.patch` and the `data.saved`.
    - On double updates, i.e. when we are updating attribute values, which
    are already saved in the DB, then `data.patch` will ignore those values,
    thus on nested fields we may overwrite already existing values, e.g.
        schema = {'obj': {'foo': string, 'bar': string}};
        data.saved = {'obj': {'foo': '42', 'bar': 'baz'}};
        data.given = {'obj': {'foo': '13', 'bar': 'baz'}};
        data.patch = {'obj': {'foo': '13'}};
    Thus 'bar' will be overwriten, because patch is missing data.
    """


@command()
def build_full_response():
    """Builds full patch from data.patch and data.saved for a response

    This solves an issue, when we data.patch is missing some data from
    data.given because that data is already in data.saved, thus response
    instead of having values from data.saved - returns default values
    (usually `None` or empty strings), which is confusing to the user.
    """


@command()
def rename_metadata():
    """Renames metadata fields to be compatible with spinta's notation.

    XXX: this is actually a hack and should be refactored into something smarter
    """


@command()
def get_model_scopes():
    """Returns list of model scopes by given list of actions.
    """
