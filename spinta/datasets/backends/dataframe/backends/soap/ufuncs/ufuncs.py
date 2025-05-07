from spinta.core.ufuncs import ufunc, Expr
from spinta.datasets.backends.dataframe.backends.soap.ufuncs.components import SoapQueryBuilder


# INFO: All input() ufuncs will come here. It's basically *args, **kwargs
@ufunc.resolver(SoapQueryBuilder, Expr, name='input')
def _input(env: SoapQueryBuilder, expr: Expr):
    pass


# Only input(source, default) will come here
@ufunc.resolver(SoapQueryBuilder, str, str, name="input")
def _input(env: SoapQueryBuilder, source: str, default: str):
    # TODO: This has to create request body that we want to use with Zeep.
    #  This will be called for each input() in DSA prepare, so we have to
    #  collect all the data into one env variable, for example env.args
    pass
