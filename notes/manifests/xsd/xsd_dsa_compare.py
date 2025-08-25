import csv
import os
import pathlib

from lxml import etree


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
        name = element.get("name")  # or etree.tostring(element).decode()
        xpaths.append(name)

    for attribute in attributes:
        xpaths.append("@" + attribute.get("name"))

    return xpaths


def compare_dsa_to_csv(directory, xsd_file_name, dsa_file_name):
    # extract all paths that xsd document describes.
    # read all sources from dsa file.
    # check that each dsa source is in the xsd and vice versa
    status = "correct"
    os.chdir(directory)
    try:
        with open(dsa_file_name, "r") as dsa_file:
            # with open(xsd_file_name, "r") as xsd_file:
            dsa_reader = csv.DictReader(dsa_file)
            dsa_sources = []
            for row in dsa_reader:
                if row["source"]:
                    dsa_sources.append(row["source"])
            # print(f"{dsa_file_name} SOURCES: ")
            # print(dsa_sources)

            sources_parts = []
            for source in dsa_sources:
                source_parts = source.split("/")
                for source_part in source_parts:
                    if source_part not in ("text()", ""):
                        sources_parts.append(source_part)

            print(f"{dsa_file_name} SOURCE PARTS: ")
            print(sources_parts)

        elements_and_attributes = extract_xpaths_from_xsd(xsd_file_name)

        for item in elements_and_attributes:
            if item is not None and item not in sources_parts:
                print(f"Item {item} in file {xsd_file_name} has not been added to dsa")
                status = "incorrect"

    except FileNotFoundError:
        print(f"file {xsd_file_name} could not be read")
        return f"file {xsd_file_name} could not be read"
    print(f"file {xsd_file_name} conversion status: {status}")

    return f"file {xsd_file_name} conversion status: {status}"


directory = "/home/karina/work/ivpk/spinta/xsds"

os.chdir(directory)
for file in os.listdir(directory):
    if file.endswith(".xsd"):
        base_name = file.replace(".xsd", "")
        dsa_file_name = f"dsa_{base_name}.csv"
        results = compare_dsa_to_csv(directory, file, dsa_file_name)

        results_file_name = "results.txt"
        with open(results_file_name, "a") as results_file:
            results_file.write(results)
            results_file.write("\n")
