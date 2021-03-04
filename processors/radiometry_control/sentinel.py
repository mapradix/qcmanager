import os

from processors.radiometry_control.base import QCProcessorRadiometryControlBase
from processors.sentinel import QCProcessorSentinelMeta
from processors.exceptions import ProcessorFailedError

from manager.logger import Logger


class QCProcessorRadiometryControlSentinel(QCProcessorRadiometryControlBase, QCProcessorSentinelMeta):

    def __init__(self, config, response):
        super(QCProcessorRadiometryControlSentinel, self).__init__(
            config, response
        )

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2A changed to LH."""
        return data_dir.replace('L2A', 'L2H')[:-5]

    def check_dependency(self):
        import numpy
        from osgeo import gdal

    def compute(self, data_dir, output_dir):
        """Compute radiometry saturation pixels

           :param str data_dir: input data directory
           s2_bands: ['B02', 'B03', 'B04', 'B08', 'B11', 'B12']
           :param str output_dir: output directory
           :return str: output file
         """
        try:
            input_file = self.filter_files(data_dir, 'MSK_SATURA_B02.gml')[0]
        except IndexError:
            raise ProcessorFailedError(
                self,
                "No GML saturation file found in {}".format(data_dir)
            )

        return self.rasterize_satura(input_file, data_dir, output_dir)

    def rasterize_satura(self, input_file, data_dir, output_dir):
        """Rasterize radiometry saturation bands.

            s2_bands = ['B02', 'B03', 'B04', 'B08', 'B11', 'B12']
            TBD: test refl. val > 10 000

        :param str input_file: input gml file
        :param str data_dir: data directory
        :param str output_dir: output directory
        :return str: output file
        """
        import numpy as np
        from osgeo import gdal, gdalconst, ogr, osr
        gdal.UseExceptions()

        try:
            # open saturation gml file
            source_ds = ogr.Open(input_file)
            source_layer = source_ds.GetLayer()

            # prepare raster file
            img_ref_file = self.filter_files(
                data_dir, 'B02_{}m.jp2'.format(self.target_res))[0]
            ids = gdal.Open(img_ref_file, gdalconst.GA_ReadOnly)
            iproj = ids.GetProjection()
            itrans = ids.GetGeoTransform()
            res = abs(itrans[1])
            img_band = ids.GetRasterBand(1)

            # open output data
            driver = gdal.GetDriverByName('GTiff')
            output_file = os.path.join(output_dir,
                                       '{}_{}m.tif'.format(self.identifier, int(res))
            )
            ods = driver.Create(output_file,
                                img_band.XSize, img_band.YSize, 1,
                                eType=gdal.GDT_Byte)
            ods.SetGeoTransform(itrans)
            ods.SetProjection(iproj)

            # create msk satura array
            msk_zeros = np.zeros((img_band.YSize, img_band.XSize), dtype=np.int)
            ods.GetRasterBand(1).WriteArray(msk_zeros)
            ods.GetRasterBand(1).SetNoDataValue(255)

            # rasterize
            if source_layer is not None:
                gdal.RasterizeLayer(ods, [1], source_layer, burn_values=[1])
            else:
                Logger.info('No saturation pixels.')
            Logger.debug("Saturation file {} created".format(output_file))

            ids = None
            ods = None

        except (RuntimeError, IndexError) as e:
            raise ProcessorFailedError(
                self,
                'Rasterization of radiometry saturation bands failed: {}'.format(e)
            )

        return output_file




