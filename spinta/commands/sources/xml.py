from lxml import etree

from spinta.commands import command
from spinta.components import Context
from spinta.types.dataset import Model, Property
from spinta.fetcher import fetch


@command()
def read_xml():
    pass


@read_xml.register()
def read_xml(context: Context, model: Model, *, source=str, dependency: dict, root: str):
    url = source.format(**dependency)
    with fetch(context, url).open('rb') as f:
        tag = root.split('/')[-1]
        context = etree.iterparse(f, tag=tag)
        for action, elem in context:
            ancestors = elem.xpath('ancestor-or-self::*')
            here = '/' + '/'.join([x.tag for x in ancestors])

            if here == root:
                yield elem

            # Clean unused elements.
            # https://stackoverflow.com/a/7171543/475477
            elem.clear()
            for ancestor in ancestors:
                while ancestor.getprevious() is not None:
                    del ancestor.getparent()[0]


@read_xml.register()
def read_xml(context: Context, prop: Property, *, source=str, value: etree.ElementBase):
    result = value.xpath(source)
    if len(result) == 1:
        return result[0]
    elif len(result) == 0:
        return None
    else:
        context.error(f"More than one value returned for {source}: {value}")
