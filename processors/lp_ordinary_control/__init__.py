import os
import sys
import osgeo
from osgeo import gdal, ogr, osr
import numpy as np


from processors import QCProcessorLPBase, identifier_from_file

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorLPOrdinaryControl(QCProcessorLPBase):
    """Land Product ordinary control processor [validation control].
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "lpOrdinaryControlMetric"
    isMeasurementOfSection = "qualityIndicators"

    def check_dependency(self):
        """Check processor's dependencies."""
        import numpy
        from osgeo import gdal

    def _run(self):
        """Perform processor's tasks.

        :return dict: QI metadata
        """
        Logger.info('Running ordinary control')
        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            'value': False
        }

        prod_raster = self.config['land_product']['product_type'][-1] + '_raster'
        lp_fn = os.path.join(self.config['map_product']['path'],
                             self.config['map_product'][prod_raster])

        lp_characteristics = self._lp_characteristics(lp_fn)
        response_data.update(lp_characteristics)
        if lp_characteristics['read'] is False:
            self.set_response_status(DbIpOperationStatus.rejected)
            return response_data

        res = self.config['land_product']['geometric_resolution']
        if lp_characteristics['xRes'] == res and lp_characteristics['xRes'] == res:
            response_data['value'] = True
        else:
            response_data['value'] = False

        if str(lp_characteristics['epsg']) == str(self.config['land_product']['epsg']):
            response_data['value'] = True
        else:
            response_data['value'] = False

        if str(lp_characteristics['dataType']) == str(self.config['land_product']['data_type']):
            response_data['value'] = True
        else:
            response_data['value'] = False

        if str(lp_characteristics['rasterFormat']) == str(self.config['land_product']['delivery_format']):
            response_data['value'] = True
        else:
            response_data['value'] = False

        if response_data['value'] is False:
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data


    def _calc_aoiCoveragePct(self, input_zone_polygon, input_value_raster,
                                  lp_min, lp_max, unclassifiable, out_of_aoi):
        """Calculate Land Product coverage statistics.

        :param str input_zone_polygon: input vector file with zones
        :param str input_value_raster: input raster value file
        :param int lp_min: LP min value
        :param float lp_max: LP max value
        :param int unclassifiable: unclassifiable value
        :param out_of_aoi: output AOI vector file
        """
        raster = gdal.Open(input_value_raster)
        shp = ogr.Open(input_zone_polygon)
        lyr = shp.GetLayer()

        # Get raster georeference info
        transform = raster.GetGeoTransform()
        xOrigin = transform[0]
        yOrigin = transform[3]
        pixelWidth = transform[1]
        pixelHeight = transform[5]

        sourceSR = lyr.GetSpatialRef()
        # gdal 2.4.2 vs. gdal 3 in docker
        if int(osgeo.__version__[0]) >= 3:
            sourceSR.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)

        feat = lyr.GetNextFeature()
        geom = feat.GetGeometryRef()

        if (geom.GetGeometryName() == 'MULTIPOLYGON'):
            count = 0
            pointsX = [];
            pointsY = []
            for polygon in geom:
                geomInner = geom.GetGeometryRef(count)
                ring = geomInner.GetGeometryRef(0)
                numpoints = ring.GetPointCount()
                for p in range(numpoints):
                    lon, lat, z = ring.GetPoint(p)
                    pointsX.append(lon)
                    pointsY.append(lat)
                count += 1
        elif (geom.GetGeometryName() == 'POLYGON'):
            ring = geom.GetGeometryRef(0)
            numpoints = ring.GetPointCount()
            pointsX = [];
            pointsY = []
            for p in range(numpoints):
                lon, lat, z = ring.GetPoint(p)
                pointsX.append(lon)
                pointsY.append(lat)

        else:
            sys.exit("ERROR: Geometry needs to be either Polygon or Multipolygon")

        xmin = min(pointsX)
        xmax = max(pointsX)
        ymin = min(pointsY)
        ymax = max(pointsY)

        xoff = int((xmin - xOrigin) / pixelWidth)
        yoff = int((yOrigin - ymax) / pixelWidth)
        xcount = int((xmax - xmin) / pixelWidth) + 1
        ycount = int((ymax - ymin) / pixelWidth) + 1

        # Memory target raster
        target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, 1, gdal.GDT_Byte)
        target_ds.SetGeoTransform((
            xmin, pixelWidth, 0,
            ymax, 0, pixelHeight,
        ))

        raster_srs = osr.SpatialReference()
        raster_srs.ImportFromWkt(raster.GetProjectionRef())
        target_ds.SetProjection(raster_srs.ExportToWkt())

        # Rasterize zone polygon to raster
        gdal.RasterizeLayer(target_ds, [1], lyr, burn_values=[1])

        banddataraster = raster.GetRasterBand(1)
        dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(np.float)

        bandmask = target_ds.GetRasterBand(1)
        datamask = bandmask.ReadAsArray(0, 0, xcount, ycount).astype(np.float)

        zoneraster = np.ma.masked_array(dataraster, np.logical_not(datamask))

        # Calculate overlay statistics
        lp_maped_px = np.sum(((zoneraster >= lp_min) & (zoneraster <= lp_max)) * 1)
        lp_out_px = np.sum(((zoneraster == unclassifiable) & (zoneraster == out_of_aoi)) * 1)

        _aoiCoveragePct = lp_maped_px / (lp_maped_px + lp_out_px) * 100.0

        return _aoiCoveragePct

    def _lp_characteristics(self, lp_fn):
        """Get LP characteristics to check.

        :param lp_fn: input lp raster file

        :return dict: lp characteristics
        """
        import numpy as np
        from osgeo import gdal, gdal_array, osr
        from osgeo import gdalconst
        gdal.UseExceptions()
        
        lp_characteristics = {}

        try:
            ids = gdal.Open(lp_fn, gdalconst.GA_ReadOnly)
            lp_characteristics['read'] = True
        except RuntimeError:
            lp_characteristics['read'] = False
            return lp_characteristics

        # Spatial resolution
        ids = gdal.Open(lp_fn, gdalconst.GA_ReadOnly)
        img_array = ids.ReadAsArray()
        geotransform = list(ids.GetGeoTransform())
        lp_characteristics['xRes'] = abs(geotransform[1])
        lp_characteristics['yRes'] = abs(geotransform[5])

        # Projection
        proj = osr.SpatialReference(wkt=ids.GetProjection())
        map_epsg = (proj.GetAttrValue('AUTHORITY', 1))
        lp_characteristics['epsg'] = map_epsg

        # Coding data type
        map_dtype = gdal_array.GDALTypeCodeToNumericTypeCode(ids.GetRasterBand(1).DataType)
        if (map_dtype == np.dtype('uint8')):
            lp_characteristics['dataType'] = 'u8'
        else:
            lp_characteristics['dataType'] = str(ids.GetRasterBand(1).DataType)

        # Map extent in the 'map_epsg'
        ulx, xres, xskew, uly, yskew, yres = ids.GetGeoTransform()
        lrx = ulx + (ids.RasterXSize * xres)
        lry = uly + (ids.RasterYSize * yres)
        lp_characteristics['extentUlLr'] = [ulx, uly, lrx, lry]

        # Do spatial overlay
        aoi_polygon = os.path.join(self.config['map_product']['path'],
                                   self.config['map_product']['map_aoi'])
        coding_val = []
        for prod in self.config['land_product']['product_type']:
            for val in self.config['land_product']['raster_coding'][prod]:
                coding_val.append(self.config['land_product']['raster_coding'][prod][val])
        lp_min = min(coding_val)
        lp_max = max(coding_val)
        unclassifiable = self.config['land_product']['raster_coding']['unclassifiable']
        out_of_aoi = self.config['land_product']['raster_coding']['out_of_aoi']
        lp_characteristics['aoiCoveragePct'] = self._calc_aoiCoveragePct(aoi_polygon, lp_fn,
                                                                         lp_min, lp_max, unclassifiable, out_of_aoi)
        # Map format
        raster_format = lp_fn.split('.')[-1]
        if raster_format == 'tif':
            raster_format = "GeoTIFF"
        lp_characteristics['rasterFormat'] = raster_format

        return lp_characteristics
