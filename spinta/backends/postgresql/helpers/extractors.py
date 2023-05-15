import re


def extract_error_property_name(error_message: str) -> str:
    pattern = re.compile(r'Key \((.*?)\)')
    return pattern.search(error_message).group(1).split(".")[0]


def extract_error_ref_id(error_message: str) -> str:
    pattern = re.compile(r'\)=\((.*?)\)')
    return pattern.search(error_message).group(1)


def extract_error_model(error_message: str) -> str:
    pattern = re.compile(r'\"([^"]*)\"')
    return pattern.search(error_message).group(1)
