from typing import Optional

from spinta.manifests.components import Manifest
import sqlalchemy as sa


class SqlManifest(Manifest):
    type = 'sql'
    path: Optional[str] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        try:
            url = sa.engine.make_url(path)
            url.get_dialect()
            return True
        except:
            return False
