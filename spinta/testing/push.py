from typing import List

import sqlalchemy as sa


def compare_push_state_rows(
    engine: sa.engine.Engine,
    table_name: str,
    rows: List[dict],
    order_by=None
):
    if order_by is None:
        order_by = ['id']

    with engine.connect() as conn:
        state_rows = conn.execute(
            sa.text(
                f'SELECT * FROM "{table_name}" ORDER BY {", ".join(order_by)}'
            )
        )

        for i, row in enumerate(state_rows):
            compare_row = rows[i]
            for key, value in compare_row.items():
                assert getattr(row, key) == value
