import os

from processors.harmonization_stack.base import \
    QCProcessorHarmonizationStackBase
from processors.sentinel import QCProcessorSentinelMeta


class QCProcessorHarmonizationStackSentinel(QCProcessorHarmonizationStackBase, QCProcessorSentinelMeta):
    """Processor creating a resampled stack for Sentinel."""

    def get_band_ids(self, data_dir):
        """Get filenames for all bands.

        :param data_dir: path to data directory
        """
        if 'L2A' in data_dir:
            # B10_60 not available, see
            # http://step.esa.int/thirdparties/sen2cor/2.5.5/docs/S2-PDGS-MPC-L2A-IODD-V2.5.5.pdf
            target_res = int(
                self.config['land_product']['geometric_resolution']
            )
            bands = [
                # available in 10/20m
                'B02_{}m.jp2'.format(target_res),
                'B03_{}m.jp2'.format(target_res),
                'B04_{}m.jp2'.format(target_res),
                'B8A_20m.jp2'.format(target_res),
                # available only in 20m
                'B11_20m.jp2', 'B12_20m.jp2' 
            ]
        else:
            bands = [
                'B02.jp2', 'B03.jp2', 'B04.jp2', 'B08.jp2',
                'B11.jp2', 'B12.jp2'
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
            ip2.replace('L2A', 'L2H')
        )

    @staticmethod
    def get_ordinary_control_processor():
        """Get platform specific processor for ordinary control."""
        from processors.harmonization_stack.ordinary_control_sentinel import  \
            QCProcessorOrdinaryControlStackSentinel

        return QCProcessorOrdinaryControlStackSentinel

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

        x_target = int(x * ref_trans[1] / ref_trans_new[1])
        y_target = int(y * ref_trans[5] / ref_trans_new[5])

        output = driver.Create(out_file, x_target, y_target, 1, data_type)
        output.SetGeoTransform(ref_trans_new)
        output.SetProjection(ref_proj)

        gdal.ReprojectImage(in_image, output, in_proj, ref_proj,
                            gdalconst.GRA_NearestNeighbour)

        output = None
