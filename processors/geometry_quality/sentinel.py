"""Sentinel-specific child class for GeometryQualityBase."""

from processors.geometry_quality.base import QCProcessorGeometryQualityBase
from processors.sentinel import QCProcessorSentinelMeta
from processors.valid_pixels.sentinel import QCProcessorValidPixelsSentinel


class QCProcessorGeometryQualitySentinel(QCProcessorGeometryQualityBase, QCProcessorSentinelMeta):
    """Processor performing geometry control for Sentinel products."""

    @staticmethod
    def bands2stack(b2s, data_dir):
        """Get band intended for the use.

        :param b2s: Value band4match from the config file
        :param data_dir: path to data directory
        """
        return 'B0{}'.format(b2s)

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2A changed to LH.

        :param data_dir: path to data directory
        """
        return data_dir.replace('L2A', 'L2H')[:-5]

    @staticmethod
    def get_nodata_values():
        """Get the platform-dependent nodata tuple."""
        return (65535, 65535)

    @staticmethod
    def get_valid_pixels_processor(config, response):
        """Get ValidPixels processor corresponding to the platform.

        :param config: processor-related config file
        :param response: processor response managed by the manager
        """
        return QCProcessorValidPixelsSentinel(config, response)
