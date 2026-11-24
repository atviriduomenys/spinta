import sqlalchemy as sa

from spinta.core.ufuncs import Expr
from spinta.manifests.components import Manifest


class SqlManifest(Manifest):
    type = "sql"
    path: str | None = None
    prepare: Expr | None = None

    @staticmethod
    def detect_from_path(path: str) -> bool:
        try:
            url = sa.engine.make_url(path)
            if not url:
                return False
            url.get_dialect()
            engine = sa.create_engine(url)
            inspector = sa.inspect(engine)
            return not inspector.has_table("_manifest")
        # A Windows path, `C:\...`, reads as a URL with an invalid port.
        except (sa.exc.SQLAlchemyError, ValueError):
            return False
