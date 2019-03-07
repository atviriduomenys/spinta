import requests
from lxml import etree

from spinta.commands import Command


class XmlDatasetModel(Command):
    metadata = {
        'name': 'xml',
        'type': 'dataset.model',
    }

    def execute(self):
        with requests.get(self.args.url, stream=True) as r:
            r.raw.decode_content = True
            tag = self.args.root.split('/')[-1]
            context = etree.iterparse(r.raw, tag=tag)
            for action, elem in context:
                ancestors = elem.xpath('ancestor-or-self::*')
                here = '/' + '/'.join([x.tag for x in ancestors])

                if here == self.args.root:
                    yield elem

                # Clean unused elements.
                # https://stackoverflow.com/a/7171543/475477
                elem.clear()
                for ancestor in ancestors:
                    while ancestor.getprevious() is not None:
                        del ancestor.getparent()[0]


class XmlDatasetProperty(Command):
    metadata = {
        'name': 'xml',
        'type': 'dataset.property',
    }

    def execute(self):
        result = self.args.value.xpath(self.args.source)
        if len(result) == 1:
            return result[0]
        elif len(result) == 0:
            return None
        else:
            self.error(f"More than one value returned for {self.args.source}: {self.args.value}")
