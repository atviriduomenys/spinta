from spinta.api import app  # noqa
from spinta.api import set_store
from spinta.config import get_config
from spinta.store import Store


config = get_config()

store = Store()
store.add_types()
store.add_commands()
store.configure(config)
store.prepare(internal=True)
store.prepare()

set_store(store)

app.debug = store.config.debug
