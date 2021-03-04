from processors.valid_pixels.base import QCProcessorValidPixelsBase
from processors.landsat import QCProcessorLandsatMeta


class QCProcessorValidPixelsLandsat(QCProcessorValidPixelsBase, QCProcessorLandsatMeta):

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2 changed to LH."""
        return data_dir.replace('LC08_L2', 'LC08_L2H')
