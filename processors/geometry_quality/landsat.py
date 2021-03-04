"""Landsat-specific child class for GeometryQualityBase."""

from processors.geometry_quality.base import QCProcessorGeometryQualityBase
from processors.landsat import QCProcessorLandsatMeta
from processors.valid_pixels.landsat import QCProcessorValidPixelsLandsat


class QCProcessorGeometryQualityLandsat(QCProcessorGeometryQualityBase, QCProcessorLandsatMeta):
    """Processor performing geometry control for Landsat products."""

    @staticmethod
    def bands2stack(b2s, data_dir):
        """Get band intended for the use.

        :param b2s: Value band4match from the config file
        :param data_dir: path to data directory
        """
        if 'L2' in data_dir:
            band_id = 'band{}'.format(b2s)
        else:
            band_id = 'B{}'.format(b2s)

        return band_id

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2 changed to L2H.

        :param data_dir: path to data directory
        """
        return data_dir.replace('LC08_L2', 'LC08_L2H')

    @staticmethod
    def get_nodata_values():
        """Get the platform-dependent nodata tuple."""
        # TODO: It should be ideally 0 for Sentinel and None for Landsat, but
        #  arosics crashes when the nodata value appears on other places than
        #  edges - new releases should be checked for the fix in the future
        nodata = (65535, 65535)
        return nodata

    @staticmethod
    def get_valid_pixels_processor(config, response):
        """Get ValidPixels processor corresponding to the platform.

        :param config: processor-related config file
        :param response: processor response managed by the manager
        """
        return QCProcessorValidPixelsLandsat(config, response)
