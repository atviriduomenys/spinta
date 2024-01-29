import contextlib
from typing import Optional, Dict, List, Final, Literal

from spinta.manifests.components import Manifest
import sqlalchemy as sa


class InternalSQLManifest(Manifest):
    type = 'internal'
    path: Optional[str] = None
    engine: sa.engine.Engine = None
    table = None

    dynamic = True

    @staticmethod
    def detect_from_path(path: str) -> bool:
        try:
            url = sa.engine.make_url(path)
            if not url:
                return False
            url.get_dialect()
            engine = sa.create_engine(url)
            inspector = sa.inspect(engine)
            return inspector.has_table('_manifest')
        except sa.exc.SQLAlchemyError:
            return False

    @contextlib.contextmanager
    def transaction(self):
        with self.engine.begin() as connection:
            yield Transaction(connection)


class Transaction:
    id: str
    errors: int

    def __init__(self, connection):
        self.connection = connection


INDEX: Final = 'index'
ID: Final = 'id'
PARENT: Final = 'parent'
DEPTH: Final = 'depth'
PATH: Final = 'path'
MPATH: Final = 'mpath'
DIM: Final = 'dim'
NAME: Final = 'name'
TYPE: Final = 'type'
REF: Final = 'ref'
SOURCE: Final = 'source'
PREPARE: Final = 'prepare'
LEVEL: Final = 'level'
ACCESS: Final = 'access'
URI: Final = 'uri'
TITLE: Final = 'title'
DESCRIPTION: Final = 'description'
InternalManifestColumn = Literal[
    'index',
    'id',
    'parent',
    'depth',
    'path',
    'mpath',
    'dim',
    'name',
    'type',
    'ref',
    'source',
    'prepare',
    'level',
    'access',
    'uri',
    'title',
    'description',
]
INTERNAL_MANIFEST_COLUMNS: List[InternalManifestColumn] = [
    INDEX,
    ID,
    PARENT,
    DEPTH,
    PATH,
    MPATH,
    DIM,
    NAME,
    TYPE,
    REF,
    SOURCE,
    PREPARE,
    LEVEL,
    ACCESS,
    URI,
    TITLE,
    DESCRIPTION,
]

InternalManifestRow = Dict[InternalManifestColumn, str]
