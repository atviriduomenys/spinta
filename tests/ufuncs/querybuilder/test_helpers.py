from spinta.components import Model, Property
from spinta.types.datatype import Denorm, Object, Ref, String
from spinta.ufuncs.components import ForeignProperty
from spinta.ufuncs.querybuilder.helpers import denorm_to_foreign_property


def _prop(parent, dtype) -> Property:
    prop = Property()
    prop.parent = parent
    prop.dtype = dtype
    dtype.prop = prop
    return prop


def _denorm(parent) -> Denorm:
    dtype = Denorm()
    _prop(parent, dtype)
    dtype.rel_prop = _prop(Model(), String())
    return dtype


def test_denorm_without_reference_is_its_related_property():
    """A denorm with no reference above it is read as the property it stands for."""
    dtype = _denorm(_prop(Model(), Object()))

    assert denorm_to_foreign_property(dtype) is dtype.rel_prop


def test_denorm_under_object_of_reference_joins_the_reference():
    """`country.address.street` is read through `country`, past the `address` object."""
    ref = Ref()
    ref.inherited = False
    country = _prop(Model(), ref)
    dtype = _denorm(_prop(country, Object()))

    result = denorm_to_foreign_property(dtype)

    assert isinstance(result, ForeignProperty)
    assert result.left is ref
    assert result.right is dtype.rel_prop.dtype
