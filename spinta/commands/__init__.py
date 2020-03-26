from spinta.dispatcher import command

from spinta.components.core import Store
from spinta.components.manifests import Manifest


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
def load_search_params():  # TODO: rename to `to_native`
    """Load search parameters as native python values.
    """


@command()
def load_operator_value():  # TODO: rename to `to_native`.
    """Loads URL query operator value given by the user.
    """


@command()
def check():
    """Validate component parameters.

    This command is called after loading components and converting parameter
    values to native python types.

    """


@command()
def validate():
    """Simple data validation.

    This command validates given data using node parameters. It command does not
    validate given data agains saved data and does not do any integrity checks
    or does not do any queries to a backend.

    This command is called before retrieving exising data from a backend.
    """


@command()
def verify():
    """Complex data validation.

    This command does a more complex validateion, compares given data with saved
    data, can do backends queris to verify data integrity.

    This command is called after existing data is retrieved from a backend.
    """


@command()
def simple_data_check():  # TODO: rename to validate
    """Run a simple data check.

    Check data by using restrictions given in schema, do not run any business
    logic. Also simple data check should not do any database queries.

    Simple data check is run early, before retrieving exisisting data from
    database.

    Later a complex data check is run with existing data loaded from database.
    """


@command()
def complex_data_check():   # TODO: rename to verify
    """Run a complex data check.

    At this point, simple data check is already done and data is passed simple
    validation.

    Complex data check will receive existing data in database, additional
    queries can be run to do more complex validations.
    """


@command()
def check_unique_constraint():  # TODO: remove
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

    - Prepare external dataset for data import:

        prepare(Context, Source, Node)

    """


@command()
def prepare_data_for_response():  # TODO: rename to `to_extern`
    """Prepare Python-native data types for response.

    Prepared data must be json-serializable.

    This command is responsible for preparing model or property data at the top
    level of data structure. While prepare_dtype_for_response is responsible for
    preparing individual properties.
    """


@command()
def prepare_dtype_for_response():  # TODO: rename to `to_extern`
    """Prepare Python-native data types for response.

    Prepared data must be json-serializable.

    This command is responsible for preparing individual properties for
    response. For the top level data preparation see prepare_data_for_response.
    """


@command()
def to_native():
    """Convert external data type to python native data type.

    to_native(Context, DataType, PostgreSQL, object)
        -> convert postgresql data type to python native data type.

    to_native(Context, DataType, Json, object)
        -> convert JSON data type to python native data type.
    """


@command()
def to_extern():
    """Convert python native data type to external data type.

    to_extern(Context, DataType, PostgreSQL, object)
        -> convert python native data type to postgresql data type.

    to_extern(Context, DataType, Json, object):
        -> convert python native data type to postgresql data type
    """


@command()
def freeze():
    """Create new schema version."""


@command()
def wait():
    """Wait while all database backends are up.

    Database backends are a separate services and once you start multiple
    services at the same time, you need to make sure, that all the required
    external services are up before running the app.
    """


@command(
    (Store, ('manifest')),
    (Manifest, ('context.store..')),
)
def init():
    """Initialize backends.

    This command is called after loading and checking backend.
    """


@command()
def bootstrap():
    """Bootstrap all components.

    Currently if database is empty instead of running all migrations, it creates
    all the tables and constrains directly.
    """


@command()
def sync():
    """Sync manifest YAML files to database."""


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
def insert_many():  # TODO: remove
    pass


@command()
def upsert():
    pass


@command()
def upsert_many():  # TODO: remove
    pass


@command()
def update():
    pass


@command()
def update_many():  # TODO: remove
    pass


@command()
def patch():
    pass


@command()
def patch_many():  # TODO: remove
    pass


@command()
def delete():
    pass


@command()
def delete_many():  # TODO: remove
    pass


@command()
def getone():
    """Get single record from the databse."""


@command()
def getfile():
    """Get full file content

    This can raise error if file is to large to read into memeory.

    This command is used for file property, to read while file content into
    `_content` property if explicitly requested. For small files this is OK, but
    for larger files direct file access via subresource API should be used.
    """


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
def get_primary_key_type():
    """Return primary key column type.

    This applyies to some backends, for example PostgreSQL, but is not used by
    other backends, like Mongo.

    PostgreSQL returns sqlalchemy.dialects.postgresql.UUID.
    """


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
def before_write():
    """Prepare patch for backend before writing

    This command converts Python-native data types to backend-native data types
    and prepares patch that is ready for writing to database tables.

    Preparation also might involve cleaning stale data on other tables, for
    example list data are written to separate tables used for searches, file
    properties writes file blockes to a separate tables and updates patch with
    block data.
    """


@command()
def after_write():
    """Run additional actions after writing to backend

    This can invorvle writing data to other tables, for examples list can save
    list items, files can save file blocks, etc.

    At this point, main resource data are saved and all referential checks will
    pass.
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


@command()
def cast_backend_to_python():
    """Convert backend native types to python native types."""
