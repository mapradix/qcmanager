from processors.harmonization_stack.ordinary_control import \
    QCProcessorOrdinaryControlStackBase
from processors.landsat import QCProcessorLandsatMeta


class QCProcessorOrdinaryControlStackLandsat(QCProcessorOrdinaryControlStackBase, QCProcessorLandsatMeta):
    """Ordinary control processor."""

    bands = ['band2', 'band3', 'band4', 'band5', 'band6', 'band7']

    @staticmethod
    def get_maximum_dtype():
        """Get maximal allowed data type."""
        return 3
