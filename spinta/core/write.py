from typing import Optional, AsyncIterator

from spinta.core.components import Node, Backend
from spinta.core.errors import UserError
from spinta.core.actions import Action
from spinta.core.data import NA


class DataItem:
    model: Node = None        # Data model.
    prop: Node = None         # Action on a property, not a whole model.
    propref: bool = False     # Action on property reference or instance.
    backend: Backend = None   # Model or property backend depending on prop and propref.
    action: Action = None     # Action.
    payload: dict = None      # Original data from request.
    given: dict = None        # Request data converted to Python-native data types.
    saved: dict = None        # Current data stored in database.
    patch: dict = None        # Patch that is going to be stored to database.
    error: UserError = None   # Error while processing data.

    def __init__(
        self,
        model: Node = None,
        prop: Node = None,
        propref: bool = False,
        backend: Backend = None,
        action: Action = None,
        payload: Optional[dict] = None,
        error: UserError = None,
    ):
        self.model = model
        self.prop = prop
        self.propref = propref
        self.backend = model.backend if backend is None and model else backend
        self.action = action
        self.payload = payload
        self.error = error
        self.given = NA
        self.saved = NA
        self.patch = NA

    def __getitem__(self, key):
        return DataSubItem(self, *(
            d.get(key, NA) if d else NA
            for d in (self.given, self.saved, self.patch)
        ))

    def copy(self, **kwargs) -> 'DataItem':
        data = DataItem()
        attrs = [
            'model',
            'prop',
            'propref',
            'backend',
            'action',
            'payload',
            'given',
            'saved',
            'patch',
            'error',
        ]
        assert len(set(kwargs) - set(attrs)) == 0
        for name in attrs:
            if name in kwargs:
                setattr(data, name, kwargs[name])
            elif hasattr(self, name):
                value = getattr(self, name)
                if isinstance(value, dict):
                    setattr(data, name, value.copy())
                else:
                    setattr(data, name, value)
        return data


class DataSubItem:

    def __init__(self, parent, given, saved, patch):
        if isinstance(parent, DataSubItem):
            self.root = parent.root
        else:
            self.root = parent
        self.given = given
        self.saved = saved
        self.patch = patch

    def __getitem__(self, key):
        return DataSubItem(self, *(
            d.get(key, NA) if d else NA
            for d in (self.given, self.saved, self.patch)
        ))

    def __iter__(self):
        yield from self.iter(given=True, saved=True, patch=True)

    def iter(self, given=False, saved=False, patch=False):
        if saved and self.saved:
            given_ = NA
            patch_ = NA
            for saved_ in self.saved:
                yield DataSubItem(self, given_, saved_, patch_)

        if (patch or given) and self.patch:
            saved_ = NA
            for given_, patch_ in zip(self.given, self.patch):
                yield DataSubItem(self, given_, saved_, patch_)

        elif given and self.given:
            saved_ = NA
            patch_ = NA
            for given_ in self.given:
                yield DataSubItem(self, given_, saved_, patch_)


DataStream = AsyncIterator[DataItem]
