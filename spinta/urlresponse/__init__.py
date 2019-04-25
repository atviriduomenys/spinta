from spinta.commands import prepare
from spinta.components import Context
from spinta.utils.response import create_http_response


class UrlResponse:
    def __init__(self, url_params, request):
        self.url_params = url_params
        self.request = request
        self.response = None

    def create_response(self, context):
        self.response = create_http_response(self.url_params, context, self.request)


@prepare.register()
def prepare(context: Context, url_response: UrlResponse) -> UrlResponse:
    url_response.create_response(context)
    return url_response
