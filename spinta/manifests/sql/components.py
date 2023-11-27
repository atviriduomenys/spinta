from typing import Optional

from spinta.core.ufuncs import Expr
from spinta.manifests.components import Manifest
import sqlalchemy as sa


class SqlManifest(Manifest):
    type = 'sql'
    path: Optional[str] = None
    prepare: Optional[Expr] = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        try:
            url = sa.engine.make_url(path)
            if not url:
                return False
            url.get_dialect()
            engine = sa.create_engine(url)
            inspector = sa.inspect(engine)
            return not inspector.has_table('_manifest')
        except sa.exc.SQLAlchemyError:
            return False
