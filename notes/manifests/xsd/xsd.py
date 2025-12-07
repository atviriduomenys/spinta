import pathlib
from urllib.parse import urlparse
from urllib.parse import parse_qsl
from urllib.parse import urljoin

import typer
import requests
from lxml import etree
from lxml import html


app = typer.Typer()


def add_url_base(base: str, urls: list[str]) -> list[str]:
    return [urljoin(base, url) for url in urls]


def download_xsd_files(
    session: requests.Session,
    base_dir: pathlib.Path,
    file_name_template: str,
    key: str,
    urls: list[str],
) -> None:
    for url in urls:
        query = dict(parse_qsl(urlparse(url).query))
        file_name = file_name_template.format(**query)
        file_path = base_dir / file_name
        response = session.get(url)
        with file_path.open("w") as file:
            file.write(response.text)
        print(f"{file_name} <- {url}")


def extract_xsd_files(session: requests.Session, xpath: str, urls: str):
    for url in urls:
        response = session.get(url)
        document_list = html.fromstring(response.text)
        return document_list.xpath(xpath)


@app.command()
def download_rc_broker_xsd_files(
    base_dir: str,
    *,
    base_url: str = "https://ws.registrucentras.lt",
):
    session = requests.Session()

    download_xsd_files(
        session,
        base_dir,
        "out_{t}.xsd",
        add_url_base(
            base_url,
            extract_xsd_files(
                session,
                "//*[contains(@href, 'out')]",
                [
                    urljoin(base_url, "/broker/info.php"),
                ],
            ),
        ),
    )

    download_xsd_files(
        session,
        base_dir,
        "rc_jar_klasif_{kla_kodas}.xsd",
        add_url_base(
            base_url,
            extract_xsd_files(
                session,
                "//*[contains(@href, 'kla_kodas')]",
                [urljoin(base_url, "/broker/xsd.klasif.php?kla_grupe=JAR")],
            ),
        ),
    )

    download_xsd_files(
        session,
        base_dir,
        "rc_jar_klasif_{kla_kodas}.xsd",
        add_url_base(
            base_url,
            extract_xsd_files(
                session,
                "//*[contains(@href, 'kla_kodas')]",
                [urljoin(base_url, "/broker/xsd.klasif.php?kla_grupe=NTR")],
            ),
        ),
    )

    download_xsd_files(
        session,
        base_dir,
        "{f}",
        add_url_base(
            base_url,
            [
                "/broker/xsd.jadis.php?f=jadis-israsas.xsd",
                "/broker/xsd.jadis.php?f=jadis-sarasas.xsd",
                "/broker/xsd.jadis.php?f=jadis-dalyvio-israsas.xsd",
            ],
        ),
    )


# ChatGPT v4 prompt:
#   Write a Python script, that would extract list of all elements and
#   attributes as xpath expressions from list of xsd files, using lxml library.
#   List of xsd files will be provided as command line arguments.
def extract_xpaths_from_xsd(path: pathlib.Path):
    """Extracts and prints all element and attribute XPaths from a given XSD file."""
    try:
        tree = etree.parse(path)
    except etree.XMLSyntaxError as e:
        print(f"Error parsing {path}: {e}")
        return []

    root = tree.getroot()
    namespaces = {"xsd": "http://www.w3.org/2001/XMLSchema"}

    # Finding all element and attribute nodes in the XSD file
    elements = root.xpath(".//xsd:element", namespaces=namespaces)
    attributes = root.xpath(".//xsd:attribute", namespaces=namespaces)

    # Generate XPath for each element and attribute
    xpaths = []

    for element in elements:
        name = element.get("name") or element.get("ref")  # or etree.tostring(element).decode()
        xpaths.append("/" + name)

    for attribute in attributes:
        xpaths.append("@" + attribute.get("name"))

    return xpaths


@app.command()
def extract_xpaths_from_xsd_files(paths: list[pathlib.Path]):
    for path in paths:
        xpaths = extract_xpaths_from_xsd(path)
        for xpath in xpaths:
            print(xpath)


if __name__ == "__main__":
    app()
