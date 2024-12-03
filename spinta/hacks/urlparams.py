from typing import Dict, Union
from spinta.components import UrlParams, Property, Model


def extract_params_sort_values(
    model: Model,
    params: UrlParams
) -> Union[type(None), Dict[str, Property]]:
    """
    A hack to iterate through sort expression without using resolvers

    return sort properties
    if key starts with '-' it means that it's descending

    if returns None, then it could not parse something
    """
    if not params.sort:
        return {}

    result = {}
    for sort_ast in params.sort:
        negative = False
        if sort_ast['name'] == 'negative':
            negative = True
            sort_ast = sort_ast['args'][0]
        elif sort_ast['name'] == 'positive':
            sort_ast = sort_ast['args'][0]
        for sort in sort_ast['args']:
            if isinstance(sort, str):
                if sort in model.properties.keys():
                    by = sort if not negative else f'-{sort}'
                    prop = model.properties[sort]
                    result[by] = prop
            else:
                return None

    return result
