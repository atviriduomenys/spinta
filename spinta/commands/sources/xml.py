from lxml import etree

from spinta.components import Context
from spinta.types.dataset import Model, Property
from spinta.fetcher import fetch
from spinta.commands.sources import Source
from spinta.commands import pull
from spinta import exceptions


class Xml(Source):
    pass


@pull.register()
def pull(context: Context, source: Xml, node: Model, *, params: dict):
    dataset = node.parent
    url = dataset.source.name.format(**params)
    with fetch(context, url).open('rb') as f:
        tag = source.name.split('/')[-1]
        context = etree.iterparse(f, tag=tag)
        for action, elem in context:
            ancestors = elem.xpath('ancestor-or-self::*')
            here = '/' + '/'.join([x.tag for x in ancestors])

            if here == source.name:
                yield elem

            # Clean unused elements.
            # https://stackoverflow.com/a/7171543/475477
            elem.clear()
            for ancestor in ancestors:
                while ancestor.getprevious() is not None:
                    del ancestor.getparent()[0]


@pull.register()
def pull(context: Context, source: Xml, node: Property, *, data: etree.ElementBase):
    result = data.xpath(source.name)
    if len(result) == 1:
        return result[0]
    elif len(result) == 0:
        return None
    else:
        raise exceptions.InvalidSource(
            node,
            source=source,
            error=f"More than one value returned for {source.name!r}: {data!r}",
        )
