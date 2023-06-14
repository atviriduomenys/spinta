import sqlalchemy as sa
from sqlalchemy.dialects import mysql

import frictionless
from geoalchemy2.types import Geometry

from spinta.datasets.backends.sql.frictionless import GeoSqlStorage
from spinta.manifests.sql.helpers import _get_type


def test_geometry_type():
    metadata = sa.MetaData()
    sa.Table('test_table', metadata, sa.Column('geom', Geometry()))
    storage = GeoSqlStorage(url='sqlite://')
    storage._SqlStorage__metadata = metadata
    package = frictionless.Package.from_storage(storage)
    assert package.resources[0].schema.fields[0].type == 'geometry'


def test_time_type():
    metadata = sa.MetaData()
    sa.Table('test_table', metadata, sa.Column('time', sa.Time()))
    storage = GeoSqlStorage(url='sqlite://')
    storage._SqlStorage__metadata = metadata
    package = frictionless.Package.from_storage(storage)
    assert package.resources[0].schema.fields[0].type == 'time'


def test_char_type():
    assert _get_type(mysql.CHAR()) == 'string'
