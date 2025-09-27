from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa


def group_array(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.json_array(*column)
    # Using concat for mssql, because `json_arrayagg` is only available for Azure SQL version
    # https://learn.microsoft.com/en-us/sql/t-sql/functions/json-array-transact-sql?view=azuresqldb-current
    return sa.sql.func.concat("[", column, "]")
