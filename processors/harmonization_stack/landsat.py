import os
import tempfile

from processors.harmonization_stack.base import \
    QCProcessorHarmonizationStackBase
from processors.landsat import QCProcessorLandsatMeta


class QCProcessorHarmonizationStackLandsat(QCProcessorHarmonizationStackBase, QCProcessorLandsatMeta):
    """Processor creating a resampled stack for Landsat."""

    def get_band_ids(self, data_dir):
        """Get filenames for all bands.

        :param data_dir: path to data directory
        """
        if 'L2' in data_dir:
            bands = [
                'sr_band2.TIF',
                'sr_band3.TIF',
                'sr_band4.TIF',
                'sr_band5.TIF',
                'sr_band6.TIF',
                'sr_band7.TIF',
            ]
        else:
            bands = [
                'B2.TIF',
                'B3.TIF',
                'B4.TIF',
                'B5.TIF',
                'B6.TIF',
                'B7.TIF',
            ]

        return bands

    def _get_ip_output_path(self, ip):
        """Get IP output path with L2 changed to L2H.

        :param ip: IP title
        """
        ip2 = self.get_processing_level2(self.get_meta_data(ip))['title']
        return os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            ip2.replace('LC08_L2', 'LC08_L2H')
        )

    @staticmethod
    def get_ordinary_control_processor():
        """Get platform specific processor for ordinary control."""
        from processors.harmonization_stack.ordinary_control_landsat import \
            QCProcessorOrdinaryControlStackLandsat

        return QCProcessorOrdinaryControlStackLandsat

    @staticmethod
    def reproject(driver, out_file, in_image, in_proj, ref_image,
                  ref_trans, ref_trans_new, ref_proj):
        """Reproject and resample a band intended resolution.

        :param driver: Driver determining the format of the written file
        :param out_file: Path to the temporary file where the resampled band
            will be created
        :param in_image: Path to the original, not-resampled band
        :param in_proj: Projection of in_image
        :param ref_image: A GDAL object representing the reference image
        :param ref_trans: Geo transform (transformation coefficients) of
            the reference image
        :param ref_trans_new: Geo transform of the reference image, where
            the resolution is changed to the intended one
        :param ref_proj: Projection of ref_image
        """
        from osgeo import gdal, gdalconst

        x = ref_image.RasterXSize
        y = ref_image.RasterYSize

        data_type = ref_image.GetRasterBand(1).DataType

        rand_file = tempfile.mkstemp(prefix='resampled_5m_', suffix='.tif')[1]

        ref_trans_list = list(ref_trans_new)
        ref_trans_list[1] = 5
        ref_trans_list[5] = -5
        ref_trans_5m = tuple(ref_trans_list)

        x_5m = int(x * ref_trans[1] / 5)
        y_5m = int(y * ref_trans[5] / -5)

        output_5m = driver.Create(rand_file, x_5m, y_5m, 1, data_type)
        output_5m.SetGeoTransform(ref_trans_5m)
        output_5m.SetProjection(ref_proj)

        gdal.ReprojectImage(in_image, output_5m, in_proj, ref_proj,
                            gdalconst.GRA_NearestNeighbour)

        # resample from 5 m to the intended resolution
        x_target = int(x * ref_trans[1] / ref_trans_new[1])
        y_target = int(y * ref_trans[5] / ref_trans_new[5])

        output = driver.Create(out_file, x_target, y_target, 1, data_type)
        output.SetGeoTransform(ref_trans_new)
        output.SetProjection(ref_proj)

        gdal.ReprojectImage(output_5m, output, in_proj, ref_proj,
                            gdalconst.GRA_NearestNeighbour)

        output_5m = None
        output = None

        # delete temporary files with single resampled bands into a 5 m res
        os.remove(rand_file)
