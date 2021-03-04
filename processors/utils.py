from osgeo import ogr


def gml2json(gml):
    """Convert GML geometry to GeoJSON.

    :param str gml: GML string

    :return str: JSON geometry
    """
    geom = ogr.CreateGeometryFromGML(gml)
    return geom.ExportToJson()


def wkt2json(wkt):
    """Convert WKT geometry to GeoJSON.

    :param str wkt: WKT string

    :return str: JSON geometry
    """
    geom = ogr.CreateGeometryFromWkt(wkt)
    return geom.ExportToJson()


def gml2bbox(gml):
    """Convert GML geometry to bbox.

    :param str gml: GML string

    :return list: bbox
    """
    geom = ogr.CreateGeometryFromGML(gml)
    # return list(geom.GetEnvelope())
    
    # get Envelope returns a tuple (minX, maxX, minY, maxY)
    # but it doesn't seems to be like that (GDAL 2.4)
    evp = geom.GetEnvelope()
    return [evp[2], evp[0], evp[3], evp[1]]


def wkt2bbox(wkt, switch_axis=False):
    """Convert WKT geometry to bbox.

    :param str wkt: WKT string
    :param bool switch_axis: switch axis (loglat -> latlog)

    :return list: bbox
    """
    geom = ogr.CreateGeometryFromWkt(wkt)
    
    # get Envelope returns a tuple (minX, maxX, minY, maxY)
    # but it doesn't seems to be like that (GDAL 2.4)
    evp = geom.GetEnvelope()
    if switch_axis:
            return [evp[2], evp[0], evp[3], evp[1]]

    return [evp[0], evp[2], evp[1], evp[3]]
