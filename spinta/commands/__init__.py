from __future__ import annotations

from typing import TYPE_CHECKING
from typing import overload

from spinta.dispatcher import command

if TYPE_CHECKING:
    from spinta.components import Store
    from spinta.backends import Backend
    from spinta.components import Context
    from spinta.manifests.components import Manifest
    from spinta.datasets.components import Dataset
    from spinta.datasets.components import Resource


@command()
def manifest_list_schemas():
    """Return iterator of manifest schema entry ids.

    For YAML manifests entry id is file path, for backend manifests, entry id is
    _id, for tabular manifests, entry id is table row id.

    """


@command()
def manifest_read_current():
    """Return current schema by given schema entry id.

    For YAML manifests, current schema is first YAML file document entry. For
    Backend manifests, current schema is read from _schema table.
    """


@command()
def manifest_read_freezed():
    """Return last freezed schema by given schema entry id.

    Freezed schema is schema of last freezed version.
    """


@command()
def manifest_read_versions():
    """Return iterator of all schema versions by given schema entry id."""


@command()
def configure():
    """Configure component before loading.

    This is a very first thing that happens to a component.
    """


@command()
def load(
    context: Context,
    manifest: Manifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
) -> None:
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
def decode():
    """Decode given value from source backend format into target backend format.

    decode(Context, SourceFormat, DataType, value)
    decode(Context, SourceBackend, DataType, value)
    decode(Context, SourceFormat, TargetBackend, DataType, value)

    """


@command()
def link():
    """Link loaded components.

    While loading, components can't be linked, because not all components might
    be loaded. When linking, all components are loaded and can be linked.
    """
    pass


@command()
def load_search_params():
    """Load search parameters as native python values.
    """


@command()
def load_operator_value():
    """Loads URL query operator value given by the user.
    """


@overload
def wait(
    context: Context,
    store: Store,
    *,
    seconds: int = None,
    verbose: bool = False,
) -> bool:
    """Wait for all backends given amount of seconds"""


@overload
def wait(context: Context, backend: Backend, *, fail: bool = False) -> bool:
    """Check if backend is available and operational

    This usually attempts to connect to backend with a very small timeout and
    return True if connection is successful or False if not.

    If `fail` is False, catch all connection related errors and return False,
    if True, raise exception as is.
    """


@command()
def wait(*args):
    """Wait while all database backends are up.

    Database backends are a separate services and once you start multiple
    services at the same time, you need to make sure, that all the required
    external services are up before running the app.
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

    - Prepare external dataset for data import:

        prepare(Context, Source, Node)

    """


@command()
def prepare_data_for_response():
    """Prepare Python-native data types for response.

    Prepared data must be json-serializable.

    This command is responsible for preparing model or property data at the top
    level of data structure. While prepare_dtype_for_response is responsible for
    preparing individual properties.
    """


@command()
def prepare_dtype_for_response():
    """Prepare Python-native data types for response.

    Prepared data must be json-serializable.

    This command is responsible for preparing individual properties for
    response. For the top level data preparation see prepare_data_for_response.
    """


@command()
def freeze():
    """Create new schema version.

    Freeze commands receive empty manifest instance, returned by create_manifest
    helper funciton. Then freeze command should fully load two versions of
    manijest, one version is current and another is freezed.

    Then these two manifests will be compared and a new version will be produced
    for each model if current differs from freezed.

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


@overload
def inspect(context: Context, manifest: Manifest):
    """Inspect whole manifest."""


@overload
def inspect(context: Context, manifest: Manifest, backend: Backend):
    """Inspect a backend defined on a manifest."""


@overload
def inspect(context: Context, dataset: Dataset):
    """Inspect dataset resources."""


@overload
def inspect(context: Context, resource: Resource, backend: Backend):
    """Inspect dataset resource."""


@command()
def inspect(*args):
    """Inspect backend schemas and update manifest.

    Usually manifest is loaded from a manifest backend, but inspect reads
    manifest metadata directly from storage backend. This way, one can generate
    manifest from an existing data source.

    Which data sources must be read will be discovered in the manifest. Inspect
    will read given manifest and the, will rewrite schemas from backend.

    This is used in inspect command.
    """
