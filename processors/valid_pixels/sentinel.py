from processors.valid_pixels.base import QCProcessorValidPixelsBase
from processors.sentinel import QCProcessorSentinelMeta


class QCProcessorValidPixelsSentinel(QCProcessorValidPixelsBase, QCProcessorSentinelMeta):

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2A changed to LH."""
        return data_dir.replace('L2A', 'L2H')[:-5]
