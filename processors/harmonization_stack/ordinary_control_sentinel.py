from processors.harmonization_stack.ordinary_control import \
    QCProcessorOrdinaryControlStackBase
from processors.sentinel import QCProcessorSentinelMeta


class QCProcessorOrdinaryControlStackSentinel(QCProcessorOrdinaryControlStackBase, QCProcessorSentinelMeta):
    """Ordinary control processor."""

    bands = ['B02', 'B03', 'B04', 'B08', 'B11', 'B12']

    @staticmethod
    def get_maximum_dtype():
        """Get maximal allowed data type."""
        return 2
