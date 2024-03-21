import operator
import itertools

from spinta import commands
from spinta.components import Context, Action, UrlParams, Model
from spinta.formats.ascii.helpers import get_widths, get_displayed_cols, draw_border, draw_header, draw_row
from spinta.manifests.components import Manifest
from spinta.formats.components import Format
from spinta.utils.nestedstruct import flatten, sepgetter
from spinta.formats.helpers import get_model_tabular_header, rename_page_col


class Ascii(Format):
    content_type = 'text/plain'
    accept_types = {
        'text/plain',
    }
    params = {
        'width': {'type': 'integer'},
        'colwidth': {'type': 'integer'},
    }

    def __call__(
        self,
        context: Context,
        model: Model,
        action: Action,
        params: UrlParams,
        data,
        width=None,
        max_col_width=None,
        max_value_length=100,
        rows_to_check=200,
        separator="  ",
    ):
        manifest: Manifest = context.get('store').manifest

        if action == Action.GETONE:
            data = [data]

        data = iter(data)
        peek = next(data, None)

        if peek is None:
            return

        data = itertools.chain([peek], data)

        if '_type' in peek:
            groups = itertools.groupby(data, operator.itemgetter('_type'))
        else:
            groups = [(None, data)]

        for name, group in groups:
            if name:
                yield f'\n\nTable: {name}\n'
                model = commands.get_model(context, manifest, name)

            rows = flatten(group, sepgetter(model))
            rows = rename_page_col(rows)
            cols = get_model_tabular_header(context, model, action, params)
            cols = [col if col != '_page' else '_page.next' for col in cols]
            read_rows, widths = get_widths(
                rows,
                cols,
                max_value_length,
                max_col_width,
                rows_to_check
            )
            rows = itertools.chain(read_rows, rows)
            if width:
                shortened, displayed_cols = get_displayed_cols(widths, width, separator)
            else:
                shortened, displayed_cols = False, cols

            yield draw_border(widths, displayed_cols, separator, shortened)
            yield draw_header(widths, displayed_cols, separator, shortened)

            memory = next(rows)
            for row in rows:
                yield draw_row(
                    {k: v for k, v in memory.items() if k != '_page.next'},
                    widths,
                    displayed_cols,
                    max_value_length,
                    separator,
                    shortened
                )
                memory = row
            yield draw_row(
                memory,
                widths,
                displayed_cols,
                max_value_length,
                separator,
                shortened
            )

            yield draw_border(widths, displayed_cols, separator, shortened)
