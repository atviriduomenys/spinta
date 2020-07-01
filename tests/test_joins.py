import pytest

from spinta.testing.client import TestClient
from spinta.testing.data import pushdata
from spinta.testing.data import listdata


def create_cities(app: TestClient, backend: str):
    """Create cities for testing joins

    +-----------+-----------+---------+
    | Continent | Country   | City    |
    +-----------+-----------+---------+
    | Europe    |           |         |
    |           | Lithuania |         |
    |           |           | Vilnius |
    |           |           | Kaunas  |
    |           | Latvia    |         |
    |           |           | Ryga    |
    +-----------+-----------+---------+

    """
    app.authmodel(f'backends/{backend}/continent', ['insert'])
    app.authmodel(f'backends/{backend}/country', ['insert'])
    app.authmodel(f'backends/{backend}/city', ['insert', 'search'])

    # Add a continent
    eu = pushdata(app, f'/backends/{backend}/continent', {
        'title': 'Europe',
    })

    # Add countries
    lt = pushdata(app, f'/backends/{backend}/country', {
        'title': 'Lithuania',
        'continent': {'_id': eu['_id']},
    })
    lv = pushdata(app, f'/backends/{backend}/country', {
        'title': 'Latvia',
        'continent': {'_id': eu['_id']},
    })

    # Add cities
    pushdata(app, f'/backends/{backend}/city', {
        'title': 'Vilnius',
        'country': {'_id': lt['_id']},
    })
    pushdata(app, f'/backends/{backend}/city', {
        'title': 'Kaunas',
        'country': {'_id': lt['_id']},
    })
    pushdata(app, f'/backends/{backend}/city', {
        'title': 'Riga',
        'country': {'_id': lv['_id']},
    })


@pytest.mark.parametrize('backend', ['postgres'])
def test_select_with_joins(app, backend):
    create_cities(app, backend)
    app.authmodel(f'backends/{backend}/city', ['search'])
    # XXX: Maybe we should require `search` scope also for linked models? Now,
    #      we only have access to `continent`, but using foreign keys, we can
    #      also access country and continent.
    resp = app.get(
        f'/backends/{backend}/city'
        '?select(title,country.title,country.continent.title)'
        '&sort(+_id)'
    )
    assert listdata(resp) == [
        ('Europe', 'Latvia', 'Riga'),
        ('Europe', 'Lithuania', 'Kaunas'),
        ('Europe', 'Lithuania', 'Vilnius'),
    ]
