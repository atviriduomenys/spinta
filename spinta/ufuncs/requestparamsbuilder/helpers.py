from spinta.components import UrlParams, ParamsPage


def disable_params_pagination(params: UrlParams):
    if params.page is not None:
        params.page.is_enabled = False
    else:
        params.page = ParamsPage(is_enabled=False)
