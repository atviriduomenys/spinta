import base64
import datetime


def fix_data_for_json(data):
    if isinstance(data, dict):
        return {k: fix_data_for_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [fix_data_for_json(v) for v in data]
    if isinstance(data, (datetime.datetime, datetime.date)):
        return data.isoformat()
    if isinstance(data, bytes):
        return base64.b64encode(data).decode('ascii')
    if isinstance(data, (int, float, str, type(None))):
        return data
    raise TypeError(f"{type(data)} probably won't serialize to JSON.")
