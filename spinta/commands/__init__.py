from __future__ import annotations

from typing import Any
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import TYPE_CHECKING
from typing import TypeVar
from typing import Union
from typing import overload

from starlette.requests import Request
from starlette.responses import Response

from spinta.typing import ObjectData
from spinta.components import Node
from spinta.components import UrlParams
from spinta.components import Version
from spinta.dispatcher import command
from spinta.manifests.components import ManifestSchema
from spinta.manifests.components import NodeSchema
from spinta.components import Namespace

if TYPE_CHECKING:
    from spinta.components import Store
    from spinta.components import Model
    from spinta.components import Property
    from spinta.types.datatype import DataType
    from spinta.components import Action
    from spinta.backends import Backend
    from spinta.components import Context
    from spinta.manifests.components import Manifest
    from spinta.datasets.components import Dataset
    from spinta.datasets.components import Resource
    from spinta.datasets.components import Entity
    from spinta.datasets.components import ExternalBackend
    from spinta.formats.components import Format
    from spinta.formats.html.components import ComplexCell
    from spinta.core.ufuncs import Expr
    from spinta.dimensions.enum.components import EnumItem


T = TypeVar('T')


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
    # Do not raise error, when trying to load a node, with a name, that is
    # already loaded, instead rename the new node.
    rename_duplicates: bool = False,
    # If True, load internal manifest, if not loaded.
    load_internal: bool = True,
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


@overload
def check(context: Context, item: EnumItem, dtype: DataType, value: Any):
    """Check enum item of a property with a data type of the property.

    Usually this checks, if enum item value is valid for a given data type.
    """


@command()
def check(*args, **kwargs):
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


@overload
def prepare(
    context: Context,
    backend: Backend,
    dtype: Manifest,
) -> None:
    """Prepare whole manifest

    Basically this initializes database schema objects, with sqlalchemy it
    constructs whole metadata schema, like tables and columns.

    This command should call prepare[Context, Backend, Property].
    """


@overload
def prepare(
    context: Context,
    backend: Backend,
    dtype: Model,
) -> None:
    """Prepare model

    This command should call prepare[Context, Backend, Property].
    """


@overload
def prepare(
    context: Context,
    backend: Backend,
    dtype: Property,
) -> Union[List[T], T, None]:
    """Prepare model property

    This command should call prepare[Context, Backend, DataType].

    Returns list of columns, single column or None.
    """


@overload
def prepare(
    context: Context,
    backend: Backend,
    dtype: DataType,
) -> Union[List[T], T, None]:
    """Prepare model property data type

    Returns list of columns, single column or None.
    """


@overload
def prepare(
    context: Context,
    params: UrlParams,
    version: Version,
    request: Request,
) -> UrlParams:
    """Prepare UrlParams from a Request."""


@command()
def prepare(*args, **kwargs):
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

    - Prepare external dataset for data import:

        prepare(Context, Source, Node)

    """


@overload
def prepare_for_write(
    context: Context,
    model: Model,
    backend: Backend,
    patch_: Dict[str, Any],
    action: Action,
):
    """Convert Python-native Model patch data to backend-native patch"""


@overload
def prepare_for_write(
    context: Context,
    prop: Property,
    backend: Backend,
    patch_: Any,
):
    """Convert Python-native Property patch data to backend-native patch

    This is called, when writing data directly into property, for example:

        PATCH /model/:one/UUID/prop

    """


@overload
def prepare_for_write(
    context: Context,
    dtype: DataType,
    backend: Backend,
    value: Any,
):
    """Convert a Python-native value to a backend-native value for a specific
    data type

    This is usually called from another prepare_for_write command, from Model or
    a Property. But also can be called recursively, for nested data types, liek
    Object or Array.
    """


@command()
def prepare_for_write(
    context: Context,
    dtype: Union[Model, Property, DataType],
    backend: Backend,
    patch_: Any,
    **kwargs,
):
    """Convert Python-native values to backend-native values

        prepare(Context, Model, Backend, dict) -> dict
        prepare(Context, Property, Backend, object) -> object
        prepare(Context, Type, Backend, object) -> object

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


@overload
def getone(
    context: Context,
    request: Request,
    node: Union[Node, Namespace],
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    pass


@overload
def getone(
    context: Context,
    request: Request,
    model: Model,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    pass


@overload
def getone(
    context: Context,
    model: Model,
    backend: Backend,
    *,
    id_: str,
) -> ObjectData:
    pass


@overload
def getone(
    context: Context,
    request: Request,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    pass


@overload
def getone(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    id_: str,
) -> ObjectData:
    pass


@command()
def getone(*args, **kwargs):
    """Get single record from the databse."""


@command()
def getfile():
    """Get full file content

    This can raise error if file is to large to read into memeory.

    This command is used for file property, to read while file content into
    `_content` property if explicitly requested. For small files this is OK, but
    for larger files direct file access via subresource API should be used.
    """


@overload
def getall(
    context: Context,
    model: Union[Model, Namespace],
    request: Request,
    *,
    action: Action,
    params: UrlParams,
) -> Response:
    pass


@overload
def getall(
    context: Context,
    model: Model,
    backend: Backend,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    pass


@overload
def getall(
    context: Context,
    external: Entity,
    backend: ExternalBackend,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    pass


@overload
def getall(
    context: Context,
    ns: Namespace,
    *,
    query: Expr = None,
    action: Optional[Action] = None,
    dataset_: Optional[str] = None,
    resource: Optional[str] = None,
):
    pass


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
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Format,
    *,
    action: Action,
    params: UrlParams,
    data: Iterator[ComplexCell],
    status_code: int = 200,
    headers: Optional[dict] = None,
):
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
def inspect(
    context: Context,
    backend: Backend,
    manifest: Optional[Manifest],
    source: Optional[Any],
) -> Iterator[ManifestSchema]:
    """Inspect manifest schema."""


@overload
def inspect(
    context: Context,
    backend: Backend,
    dataset: Optional[Dataset],
    source: Optional[Any],
) -> Iterator[ManifestSchema]:
    """Inspect dataset schema."""


@overload
def inspect(
    context: Context,
    backend: Backend,
    resource: Optional[Resource],
    source: Optional[Any],
) -> Iterator[ManifestSchema]:
    """Inspect dataset resource schema."""


@overload
def inspect(
    context: Context,
    backend: Backend,
    model: Optional[Model],
    source: Optional[Any],
) -> Iterator[ManifestSchema]:
    """Inspect model schema."""


@overload
def inspect(
    context: Context,
    backend: Backend,
    prop: Optional[Property],
    source: Optional[Any],
) -> NodeSchema:
    """Inspect model property schema."""


@overload
def inspect(
    context: Context,
    backend: Backend,
    dtype: Optional[DataType],
    source: Optional[Any],
) -> NodeSchema:
    """Inspect property data type schema."""


@command()
def inspect(*args) -> Iterator[ManifestSchema]:
    """Inspect schema of a node on a given backend.

    Usually manifest is loaded from a manifest backend, but inspect reads
    manifest metadata directly from storage backend. This way, one can generate
    manifest from an existing data source.

    Which data sources must be read will be discovered in the manifest. Inspect
    will read given manifest and then will rewrite schemas from backend.

    This is used in inspect command.

    All inspect commands takes backend and an existing node if a node is already
    in manifest. If node is not in manifest, then None will be given.

    Inspect commands must generate ManifestSchema objects for all inspected
    schema elements from a given backend, but also must return existing nodes
    even if they do not exist in the given backend.

    Nodes that exists in manifest, but not in backend, should clear source
    (external) values, indicating, that node is not present in the backend.

    If node exists on both, backend and manifest, then two nodes must be merged
    into one.

    When merging, backend should overwrite everything related to backend,
    leaving everything else as was in manifest.

    Existing manifest nodes must be matched with backend equivalents by
    comparing source names.

    """
