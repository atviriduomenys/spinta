from typing import Dict
from typing import Optional
from typing import Type

from spinta import commands
from spinta.components import Config
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.datasets.keymaps.components import KeyMap


def load_keymaps(
    context: Context,
    *,
    backends: Optional[Dict[str, Type[KeyMap]]] = None,
) -> Dict[str, KeyMap]:
    keymaps = {}
    if backends is None:
        config: Config = context.get('config')
        backends = config.components['keymaps']
    rc: RawConfig = context.get('rc')
    for name in rc.keys('keymaps'):
        backend = rc.get('keymaps', name, 'type', required=True)
        Backend = backends[backend]
        keymap = keymaps[name] = Backend()
        keymap.name = name
        commands.configure(context, keymap)
    return keymaps


def load_keymap_from_url(
    context: Context,
    url: str,
    *,
    backends: Optional[Dict[str, Type[KeyMap]]] = None,
) -> KeyMap:
    if backends is None:
        config = context.get('config')
        backends = config.components['keymaps']
    for Backend in backends.values():
        if Backend.detect_from_url(url):
            keymap = Backend()
            keymap.name = 'default'
            rc: RawConfig = context.get('rc')
            rc = rc.fork({
                'keymaps': {
                    keymap.name: {
                        'type': keymap.type,
                        'dsn': url,
                    }
                }
            })
            with context:
                context.set('rc', rc)
                commands.configure(context, keymap)
            return keymap
    raise RuntimeError(f"Keymap backend for {url!r} not found.")
