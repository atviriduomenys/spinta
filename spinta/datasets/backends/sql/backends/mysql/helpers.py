from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa


def group_array(column: Union[sa.Column, Sequence[sa.Column]]):
    if isinstance(column, Sequence) and not isinstance(column, str):
        column = sa.sql.func.json_array(*column)
    return sa.sql.func.json_arrayagg(column)
