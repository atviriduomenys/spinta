import os
import re
import zipfile
import xml.etree.ElementTree as ET

from pdfminer.high_level import extract_text

from spinta.exceptions import InvalidAdocError

MANIFEST_FILE_PATH = "META-INF/manifest.xml"

NAMESPACE_URI = "urn:oasis:names:tc:opendocument:xmlns:manifest:1.0"
MANIFEST_NAMESPACE = {"manifest": NAMESPACE_URI}
PDF_MEDIA_TYPE = "application/pdf"

ATTR_FULL_PATH = f"{{{NAMESPACE_URI}}}full-path"
ATTR_MEDIA_TYPE = f"{{{NAMESPACE_URI}}}media-type"
MANIFEST_FILE_ENTRY_TAG = "manifest:file-entry"

TEMP_PDF_PATH = "/tmp/_extracted_temp.pdf"

SCOPES_REGEX = r"\buapi:/\S+"


def get_pdf_path_in_adoc(adoc_archive: zipfile.ZipFile) -> str:
    with adoc_archive.open(MANIFEST_FILE_PATH) as manifest_file:
        tree = ET.parse(manifest_file)
        root = tree.getroot()
        file_entries = root.findall(MANIFEST_FILE_ENTRY_TAG, MANIFEST_NAMESPACE)

        for entry in file_entries:
            full_path = entry.attrib.get(ATTR_FULL_PATH)
            media_type = entry.attrib.get(ATTR_MEDIA_TYPE)

            if media_type == PDF_MEDIA_TYPE and full_path in adoc_archive.namelist():
                return full_path
    raise InvalidAdocError("Invalid ADOC file: no pdf file found")


def extract_elements_from_adoc(adoc_path: str, regex: str) -> list[str]:
    try:
        with zipfile.ZipFile(adoc_path, "r") as adoc_archive:
            pdf_path = get_pdf_path_in_adoc(adoc_archive)

            with adoc_archive.open(pdf_path) as pdf_file:
                with open(TEMP_PDF_PATH, "wb") as out_file:
                    out_file.write(pdf_file.read())

        text = extract_text(TEMP_PDF_PATH)
        compiled = re.compile(regex)
        return compiled.findall(text)

    except (zipfile.BadZipFile, ET.ParseError, KeyError) as error:
        raise InvalidAdocError(f"Invalid ADOC file: {error}") from error
    finally:
        if os.path.exists(TEMP_PDF_PATH):
            os.remove(TEMP_PDF_PATH)
