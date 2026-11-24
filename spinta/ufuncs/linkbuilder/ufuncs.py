import logging

from spinta.components import Property
from spinta.core.ufuncs import Bind, ufunc
from spinta.datasets.backends.wsdl.components import WsdlBackend
from spinta.datasets.components import Resource
from spinta.exceptions import InvalidSource, InvalidValue, ParamNotFound
from spinta.ufuncs.linkbuilder.components import LinkBuilder

log = logging.getLogger(__name__)


@ufunc.resolver(LinkBuilder, Bind)
def wsdl(env: LinkBuilder, parent_resource_bind: Bind) -> None:
    resource_name = parent_resource_bind.name

    if resource_name == env.resource.name:
        raise InvalidValue(message=f"wsdl() argument {resource_name} must be name of another wsdl type resource.")

    wsdl_resources = (res_name for res_name, res in env.dataset.resources.items() if res.backend.type == "wsdl")
    if resource_name not in wsdl_resources:
        raise InvalidValue(message=f"wsdl() argument {resource_name} must be wsdl type resource.")

    return env.call("wsdl", env.dataset.resources[resource_name].backend)


@ufunc.resolver(LinkBuilder, WsdlBackend)
def wsdl(env: LinkBuilder, parent_resource_backend: WsdlBackend) -> None:
    soap_source = env.resource.backend.config.get("dsn")
    backend = env.resource.backend
    try:
        backend.service, backend.port, _, backend.operation = soap_source.split(".")
    except ValueError:
        error_msg = (
            f'Model source "{soap_source}" format is invalid. '
            f'Source must be provided in the following format: "service.port.port_type.operation"'
        )
        raise InvalidSource(env.resource, error=error_msg)

    backend.wsdl_backend = parent_resource_backend


@ufunc.resolver(LinkBuilder)
def param(env: LinkBuilder) -> None:
    return env.call("param", env.this)


@ufunc.resolver(LinkBuilder, Bind)
def param(env: LinkBuilder, bind: Bind) -> None:
    return env.call("param", env.this, bind)


@ufunc.resolver(LinkBuilder, Property)
def param(env: LinkBuilder, prop: Property) -> None:
    """Link source-less property to a resource param with the same name: `param()`.

    Example manifest:

        d | r | b | m | property | type    | ref    | source | prepare  | access
        example                  | dataset |        |        |          |
          | resource             | json    |        | url    |          |
          |   |   |   |          | param   | status | ST     | query()  | open
          |   |   | Country      |         |        |        |          | open
          |   |   |   | id       | integer |        | id     |          |
          |   |   |   | status   | string  |        |        | param()  |

    Example UAPI request:

        /example/Country?status=ok

    Example Data source request:

        url?ST=ok

    """
    return env.call("param", prop, Bind(prop.name))


@ufunc.resolver(LinkBuilder, str)
def param(env: LinkBuilder, name: str) -> None:
    return env.call("param", env.this, Bind(name))


@ufunc.resolver(LinkBuilder, Resource, Bind)
def param(env: LinkBuilder, _: Resource, bind: Bind) -> None:
    """Resource-level `param(name)`: return the resource param object by name."""
    if bind.name not in env.params:
        raise ParamNotFound(param=bind.name)
    return env.params[bind.name]


@ufunc.resolver(LinkBuilder, Property, str)
def param(env: LinkBuilder, prop: Property, name: str) -> None:
    return env.call("param", prop, Bind(name))


@ufunc.resolver(LinkBuilder, Property, Bind)
def param(env: LinkBuilder, prop: Property, bind: Bind) -> None:
    """Link source-less property to an explicitly named resource param: `param('name')` or `param(name)`.

    Example manifest:

        d | r | b | m | property | type    | ref    | source | prepare    | access
        example                  | dataset |        |        |            |
          | resource             | json    |        | url    |            |
          |   |   |   |          | param   | status | ST     | query()    | open
          |   |   | Country      |         |        |        |            | open
          |   |   |   | id       | integer |        | id     |            |
          |   |   |   | status   | string  |        |        | param(st)  |

    """
    if bind.name not in env.params:
        raise ParamNotFound(param=bind.name)
    prop.param = env.params[bind.name]
