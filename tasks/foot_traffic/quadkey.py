import math


def quadkey2tile(quadkey):
    z = len(quadkey)

    x = 0
    y = 0
    for i in range(len(quadkey)):
        x = x * 2
        if quadkey[i] in ['1', '3']:
            x = x + 1

        y = y * 2
        if quadkey[i] in ['2', '3']:
            y = y + 1

    y = (1 << z) - y - 1

    return z, x, y


def tile2lnglat(z, x, y):
    n = 2.0 ** z

    lon = x / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
    lat = - math.degrees(lat_rad)

    return lon, lat


def latlng2tile(lng, lat, z):
    x = math.floor((lng + 180) / 360 * math.pow(2, z))
    y = math.floor((1 - math.log(math.tan(lat * math.pi / 180) + 1 / math.cos(lat * math.pi / 180)) / math.pi) / 2 * math.pow(2, z))
    y = (1 << z) - y - 1

    return z, x, y


def tile2bounds(z, x, y):
    lon0, lat0 = tile2lnglat(z, x, y)
    lon1, lat1 = tile2lnglat(z, x+1, y+1)

    return lon0, lat0, lon1, lat1


def tile2quadkey(z, x, y):
    quadkey = ''
    y = (2**z - 1) - y
    for i in range(z, 0, -1):
        digit = 0
        mask = 1 << (i-1)
        if (x & mask) != 0:
            digit += 1
        if (y & mask) != 0:
            digit += 2
        quadkey += str(digit)

    return quadkey
