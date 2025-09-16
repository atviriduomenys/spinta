from spinta import commands
from spinta.components import Context
from spinta.types.text.components import Text


@commands.check.register(Context, Text)
def check(context: Context, dtype: Text):
    for lang in dtype.langs.values():
        commands.check(context, lang)
