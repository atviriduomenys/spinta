from urllib.parse import urljoin

import click
import requests
from faker import Faker

faker = Faker()


@click.group()
def main():
    pass


@main.group()
def create():
    pass


@main.command('orgs')
@click.option('-n', type=int, default=1000, help="Number of countries to create.")
def create_orgs(n: int):
    session = Session('http://127.0.0.1:9000')
    resp = session.post('country', json={
        'code': 'lt',
        'title': 'Lithuania',
    })
    resp.raise_for_status()
    country = resp.json()
    for i in range(n):
        session.post('org', json={
            'country': {'_id': country['_id']},
            'govid': faker.numerify('#' * 11),
            'title': faker.company(),
        })


class Session(requests.Session):

    def __init__(self, prefix=None):
        super().__init__()
        self.prefix = prefix

    def request(self, method, url, *args, **kwargs):
        url = urljoin(self.prefix, url)
        return super().request(method, url, *args, **kwargs)


if __name__ == '__main__':
    main()
