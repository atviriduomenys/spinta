import frictionless
import sqlalchemy as sa
from geoalchemy2.types import Geometry
from spinta.datasets.backends.sql.frictionless import GeoSqlStorage


def test_geometry_type(mocker):
    metadata = sa.MetaData()
    sa.Table('test_table', metadata, sa.Column('geom', Geometry()))
    storage = GeoSqlStorage(url='sqlite://')
    storage._SqlStorage__metadata = metadata
    package = frictionless.Package.from_storage(storage)
    assert package.resources[0].schema.fields[0].type == 'geometry'
