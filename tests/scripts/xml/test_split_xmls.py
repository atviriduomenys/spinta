from scripts.xml.split_xmls import split_xml_documents, XML_DEFINITION


def test_split_multiple_xml_documents():
    input_content = f"""
{XML_DEFINITION}
<root>
    <item>1</item>
</root>
{XML_DEFINITION}
<root>
    <item>2</item>
</root>
{XML_DEFINITION}
<root>
    <item>3</item>
</root>
""".strip()

    documents = split_xml_documents(input_content)

    # Should produce 3 separate documents
    assert len(documents) == 3

    # Each document should start with the XML declaration
    for doc in documents:
        assert doc.startswith(XML_DEFINITION)

    # The inner content should be preserved
    assert "<item>1</item>" in documents[0]
    assert "<item>2</item>" in documents[1]
    assert "<item>3</item>" in documents[2]
