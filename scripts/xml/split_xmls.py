"""
Split multiple XML documents stored in a single file into separate XML files.

This script reads an input XML file that contains multiple XML documents concatenated together,
where each document starts with an XML declaration (see constant: `XML_DEFINITION`).

Each XML document is extracted and written into its own file.

Input:
    - examples.xml (must be located in the same directory as this script)

Output:
    - An "output/" directory containing:
        examples1.xml
        examples2.xml
        examples3.xml
        ...

Usage:
    `python split_xmls.py`

Notes:
    - The output directory is created automatically if it does not exist.
    - Files are numbered sequentially starting from 1.
    - The XML declaration is preserved in every output file.
"""

import os


INPUT_FILE = "examples.xml"
OUTPUT_FOLDER = "output"
XML_DEFINITION = '<?xml version="1.0" encoding="UTF-8"?>'


def split_xml_documents(content: str) -> list[str]:
    content = content.strip()
    parts = [part for part in content.split(XML_DEFINITION) if part.strip()]
    return [f"{XML_DEFINITION}\n{part.strip()}\n" for part in parts]


def main() -> None:
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    with open(INPUT_FILE, "r", encoding="utf-8") as file:
        content = file.read()

    documents = split_xml_documents(content)

    for index, xml in enumerate(documents, start=1):
        filename = os.path.join(OUTPUT_FOLDER, f"examples{index}.xml")
        with open(filename, "w", encoding="utf-8") as file:
            file.write(xml)


if __name__ == "__main__":
    main()
