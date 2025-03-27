from __future__ import annotations

from spinta.datasets.backends.sql.ufuncs.query.components import SqlQueryBuilder
from spinta.types.datatype import Ref, ExternalRef
from spinta.ufuncs.querybuilder.components import Selected


def select_ref_foreign_key_properties(
    env: SqlQueryBuilder,
    dtype: Ref,
    *,
    properties: list = None
) -> dict[str, Selected]:
    if properties is None:
        if dtype.prop.external and dtype.prop.external.prepare:
            properties = env(this=dtype.prop).resolve(dtype.prop.external.prepare)

    prep = {}
    if properties is None:
        if len(dtype.refprops) != 1:
            raise Exception("Unable to map source with ref type", dtype.prop, dtype.refprops)

        table = env.backend.get_table(env.model)
        column = env.backend.get_column(table, dtype.prop, select=True)
        refprop = dtype.refprops[0]
        prep[refprop.name] = Selected(item=env.add_column(column), prop=refprop)
    else:
        if len(properties) != len(dtype.refprops):
            raise Exception("CANNOT MAP LIST WITH REFPROPS", properties, dtype.refprops)
        for i, prop in enumerate(dtype.refprops):
            sel = env.call('select', properties[i])
            if sel.prop is None:
                sel.prop = prop
            prep[prop.name] = sel
    return prep


def select_external_ref_foreign_key_properties(
    env: SqlQueryBuilder,
    dtype: ExternalRef,
    *,
    properties: list = None,
    target: str = None
) -> dict[str, Selected]:
    if properties is None:
        if dtype.prop.external and dtype.prop.external.prepare:
            properties = env(this=dtype.prop).resolve(dtype.prop.external.prepare)

    if target is not None:
        value = next((prop for prop in dtype.refprops if prop.name == target), None)
        if value is None:
            raise Exception("COULD NOT FIND TARGET IN REFPROPS", target, dtype.refprops)
        target = value

    prep = {}
    if properties is None:
        table = env.backend.get_table(env.model)
        column = env.backend.get_column(table, dtype.prop, select=True)

        if len(dtype.refprops) != 1:
            raise Exception("Unable to map source with ref type", dtype.prop, dtype.refprops)

        refprop = dtype.refprops[0]
        prep[refprop.name] = Selected(item=env.add_column(column), prop=refprop)
    else:
        if len(properties) != len(dtype.refprops):
            raise Exception("CANNOT MAP LIST WITH REFPROPS", properties, dtype.refprops)

        if target is not None:
            pos = dtype.refprops.index(target)
            sel = env.call('select', properties[pos])
            if sel.prop is None:
                sel.prop = target
            prep[target.name] = sel
        else:
            for i, prop in enumerate(dtype.refprops):
                sel = env.call('select', properties[i])
                if sel.prop is None:
                    sel.prop = prop
                prep[prop.name] = sel
    return prep
