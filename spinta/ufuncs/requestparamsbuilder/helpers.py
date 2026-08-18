from spinta.components import ParamsPage, UrlParams


def disable_params_pagination(params: UrlParams):
    if params.page is not None:
        params.page.is_enabled = False
    else:
        params.page = ParamsPage(is_enabled=False)
