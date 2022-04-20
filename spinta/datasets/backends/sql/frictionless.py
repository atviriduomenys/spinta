from geoalchemy2.types import Geometry
from frictionless.plugins.sql import SqlStorage


class GeoSqlStorage(SqlStorage):

    def _SqlStorage__read_convert_type(self, sql_type=None):
        if isinstance(sql_type, Geometry):
            return 'geometry'

        return super()._SqlStorage__read_convert_type(sql_type)
