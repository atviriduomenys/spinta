from responses import GET


def test_prepare(app, context, responses):
    responses.add(
        GET, 'http://example.com/countries.csv',
        status=200, stream=True, content_type='text/plain; charset=utf-8',
        body='kodas,šalis\nfoo,Lithuania\n',
    )

    context.pull('csv')
