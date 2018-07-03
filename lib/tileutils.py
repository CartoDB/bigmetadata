import math


def tile2lnglat(z, x, y):
    n = 2.0 ** z
    y = (1 << z) - y - 1

    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = - math.degrees(lat_rad)

    return lon, lat


def tile2bounds(z, x, y):
    lon0, lat0 = tile2lnglat(z, x, y)
    lon1, lat1 = tile2lnglat(z, x+1, y-1)

    return [lon0, lat0, lon1, lat1]
