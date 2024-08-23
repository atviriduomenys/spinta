# round_point("POINT(0.855556 5.100545)", 2) -> "POINT(0.86 5.1)"
def round_point(point: str, precision: int):
    if not '.' in point:
        return point

    prefix = 'POINT'
    if 'POINT ' in point:
        prefix = 'POINT '

    converted_point = point.replace(prefix, '')
    converted_point = converted_point.strip().removeprefix('(').removesuffix(')')
    split = converted_point.split(' ')
    if len(split) == 2:
        lat = float(split[0])
        lon = float(split[1])

        rounded_lat = round(lat, precision)
        rounded_lon = round(lon, precision)

        return f"{prefix}({rounded_lat} {rounded_lon})"
    return point


# round_url("'https://www.openstreetmap.org/?'
#     'mlat=54.68569111173754&'
#     'mlon=25.286688302053335'
#     '#map=19/54.68569111173754/25.286688302053335'", 2) ->
#     "'https://www.openstreetmap.org/?'
#     'mlat=54.69&'
#     'mlon=25.29'
#     '#map=19/54.69/25.29'"
def round_url(url: str, precision: int):
    converted_url = url.replace('https://www.openstreetmap.org/?', '')
    if not '.' in converted_url:
        return url

    split = converted_url.split('/')
    if len(split) == 3:
        lat = float(split[1])
        lon = float(split[2])

        rounded_lat = round(lat, precision)
        rounded_lon = round(lon, precision)

        return f"https://www.openstreetmap.org/?mlat={rounded_lat}&mlon={rounded_lon}#map=19/{rounded_lat}/{rounded_lon}"
    return url
