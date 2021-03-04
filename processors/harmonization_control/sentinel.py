from processors.harmonization_control.base import QCProcessorHarmonizationControlBase
from processors.sentinel import QCProcessorSentinelMeta

class QCProcessorHarmonizationControlSentinel(QCProcessorHarmonizationControlBase, QCProcessorSentinelMeta):
    """Processor performing harmonization control for Sentinel products."""
    pass
