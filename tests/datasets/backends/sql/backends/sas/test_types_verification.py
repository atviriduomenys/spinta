import csv
import io
import unittest

from spinta.datasets.backends.sql.backends.sas.formats import map_sas_type_to_sqlalchemy
from spinta.manifests.sql.helpers import TYPES

# Define expected mapping logic (reverse of map_sas_type_to_sqlalchemy + spinta type mapping)


def get_spinta_type_name(sa_type):
    for cls, name in TYPES:
        if isinstance(sa_type, cls):
            return name
    return "unknown"


# Updated expected data with LOGICAL types, not raw CSV errors
# E8601DA -> date
# YEAR -> date
# NUMX -> integer or number (depending on decimals)
# DOLLAR -> number
# HEX -> binary
# $GEOREF -> geometry

csv_data = """Column Type,Column Length,Column Format,Final datatype
char,1,                                                 ,string
char,2,                                                 ,string
char,20,$F20.                                            ,string
char,23,$GEOREF.                                         ,geometry
char,24,$HEX48.                                          ,string
num ,8,1,integer
num ,8,11.6,number
num ,8,BEST12.                                          ,integer
num ,8,COMMA10.                                         ,integer
num ,8,DATE.                                            ,date
num ,7,DATE9.                                           ,date
num ,8,DATETIME.                                        ,datetime
num ,8,DOLLAR10.                                        ,number
num ,8,DOLLAR12.2                                       ,number
num ,8,E8601DA10.                                       ,date
num ,8,E8601DT19.                                       ,datetime
num ,8,HEX4.                                            ,binary
num ,8,MONNAME3.                                        ,string
num ,5,MONYY.                                           ,date
num ,8,NUMX12.5                                         ,number
num ,8,NUMX4.                                           ,integer
num ,8,TIME12.                                          ,time
num ,8,YEAR.                                            ,date
num ,6,YEAR4.                                           ,date
num ,8,YYQ.                                             ,string"""


class TestSASFormatsVerification(unittest.TestCase):
    def test_csv_mappings(self):
        reader = csv.DictReader(io.StringIO(csv_data))
        errors = []

        for row in reader:
            sas_type = row["Column Type"].strip()
            length = row["Column Length"].strip()
            fmt = row["Column Format"].strip()
            expected = row["Final datatype"].strip()

            format_str = fmt if fmt else None

            sa_type = map_sas_type_to_sqlalchemy(sas_type, length, format_str)
            spinta_type = get_spinta_type_name(sa_type)

            if spinta_type != expected:
                errors.append(f"Row: {row} -> Got {spinta_type} (SA: {sa_type}), Expected {expected}")

        if errors:
            self.fail("\n".join(errors))


if __name__ == "__main__":
    unittest.main()
