from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Iterable, List

from spinta import exceptions
from spinta.components import Config
from spinta.components import Context
from spinta.components import Model
from spinta.components import Property
from spinta.core.enums import Access, load_level, Level
from spinta.dimensions.comments.components import Comment, CommentGiven
from spinta.exceptions import BackendNotFound
from spinta.exceptions import InvalidName
from spinta.types import TYPE_OBJECT
from spinta.utils.naming import is_valid_model_name, is_valid_property_name

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from spinta.types.datatype import DataType

ALLOWED_RESERVED_PROPERTY_NAMES = {
    "_id",
    "_created",
    "_updated",
    "_revision",
    "_label",
}

RESERVED_PROPERTY_NAMES = {
    *ALLOWED_RESERVED_PROPERTY_NAMES,
    "_op",
    "_type",
    "_txn",
    "_cid",
    "_where",
    "_base",
    "_uri",
    "_page",
    "_same_as",
}

C_LANG = "C"

SYSTEMIC_COMMENT_AUTHOR = "author"


def check_no_extra_keys(dtype: DataType, schema: Iterable, data: Iterable):
    unknown = set(data) - set(schema)
    if unknown:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(
                dtype.prop,
                property=f"{dtype.prop.place}.{prop}",
            )
            for prop in sorted(unknown)
        )


def set_dtype_backend(dtype: DataType):
    if dtype.backend:
        backends = dtype.prop.model.manifest.store.backends
        if isinstance(dtype.backend, str):
            if dtype.backend not in backends:
                raise BackendNotFound(dtype, name=dtype.backend)
            dtype.backend = backends[dtype.backend]
    else:
        dtype.backend = dtype.prop.model.backend


def replace_undeclared_ref_with_object(
    context: Context,
    prop: Property,
    original_type: str,
    original_model: str,
    refprops: List[str] | None = None,
) -> None:
    from spinta.types.datatype import Object

    ref_str = f"{original_model}[{','.join(refprops)}]" if refprops else original_model

    logger.warning(
        "Model %r referenced by %r property %r was not found in the manifest. Downgrading to object (level 2).",
        original_model,
        original_type,
        prop.place,
    )

    new_dtype = Object()
    new_dtype.name = TYPE_OBJECT
    new_dtype.type = TYPE_OBJECT
    new_dtype.type_args = []
    new_dtype.prop = prop
    new_dtype.properties = {}
    prop.dtype = new_dtype
    set_dtype_backend(new_dtype)

    load_level(context, prop, Level.structured)

    if prop.comments is None:
        prop.comments = []

    prop.comments.append(
        Comment(
            id=None,
            parent="type",
            author=SYSTEMIC_COMMENT_AUTHOR,
            access=Access.private,
            created=datetime.now(timezone.utc).isoformat(),
            comment="",
            given=CommentGiven(access=None),
            prepare=f'update(type:"{original_type}", ref:"{ref_str}")',
            level=4,
        )
    )


def check_model_name(context: Context, model: Model):
    config: Config = context.get("config")
    if config.check_names:
        if "/" in model.name:
            name = model.name.rsplit("/", 1)[1]
        else:
            name = model.name
        if name not in ["_ns", "_txn", "_schema", "_schema/Version"]:
            if not is_valid_model_name(name):
                raise InvalidName(model, name=name, type="model")


def check_property_name(context: Context, prop: Property):
    config: Config = context.get("config")
    if config.check_names:
        if prop.name.startswith("_"):
            if prop.explicitly_given:
                if prop.name in ALLOWED_RESERVED_PROPERTY_NAMES:
                    return  #  prop.name atėjo iš DSA ir yra mažajame  sąraše
            elif prop.name in RESERVED_PROPERTY_NAMES:
                return  # prop.name sugeneruotas sistemos (nėra DSA) ir yra dyžiajame sąraše
        elif is_valid_property_name(prop.name):
            return
        # "C" as default language: dataset1/City?select(id,name@lt,name@pl,name@C)
        elif prop.name == C_LANG:
            return
        raise InvalidName(prop, name=prop.name, type="property")
