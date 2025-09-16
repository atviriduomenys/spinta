from spinta import commands, exceptions
from spinta.backends import Backend
from spinta.components import Context, Config
from spinta.core.config import RawConfig
from spinta.datasets.backends.dataframe.backends.json.components import Json
from spinta.datasets.backends.dataframe.backends.memory.components import MemoryDaskBackend
from spinta.datasets.backends.dataframe.backends.xml.components import Xml
from spinta.datasets.backends.sql.components import Sql
from spinta.manifests.dict.components import JsonManifest, XmlManifest
from spinta.manifests.memory.components import MemoryManifest
from spinta.manifests.sql.components import SqlManifest
from spinta.manifests.tabular.components import CsvManifest
from spinta.datasets.backends.dataframe.backends.csv.components import Csv
from spinta.manifests.helpers import get_manifest_from_type


@commands.backend_to_manifest_type.register(Context, str)
def backend_to_manifest_type(context: Context, backend_type: str):
    config: Config = context.get('config')

    if backend_type not in config.components['backends']:
        raise exceptions.BackendNotFound(name=backend_type)
    Backend_ = config.components['backends'][backend_type]
    backend: Backend = Backend_()
    backend.type = backend_type
    return commands.backend_to_manifest_type(context, backend)


@commands.backend_to_manifest_type.register(Context, Backend)
def backend_to_manifest_type(context: Context, backend: Backend):
    rc: RawConfig = context.get('rc')
    return get_manifest_from_type(rc, backend.type)


@commands.backend_to_manifest_type.register(Context, Csv)
def backend_to_manifest_type(context: Context, backend: Csv):
    return CsvManifest


@commands.backend_to_manifest_type.register(Context, Json)
def backend_to_manifest_type(context: Context, backend: Json):
    return JsonManifest


@commands.backend_to_manifest_type.register(Context, Xml)
def backend_to_manifest_type(context: Context, backend: Xml):
    return XmlManifest


@commands.backend_to_manifest_type.register(Context, Sql)
def backend_to_manifest_type(context: Context, backend: Sql):
    return SqlManifest


@commands.backend_to_manifest_type.register(Context, MemoryDaskBackend)
def backend_to_manifest_type(context: Context, backend: MemoryDaskBackend):
    return MemoryManifest
