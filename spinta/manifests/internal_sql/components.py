from typing import Optional, Dict, List, Final, Literal

from spinta.manifests.components import Manifest
import sqlalchemy as sa


class InternalSQLManifest(Manifest):
    type = 'internal_sql'
    path: Optional[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        try:
            url = sa.engine.make_url(path)
            url.get_dialect()
            return True
        except:
            return False


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
ManifestColumn = Literal[
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
MANIFEST_COLUMNS: List[ManifestColumn] = [
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

ManifestRow = Dict[ManifestColumn, str]
