from processors.harmonization_control.base import QCProcessorHarmonizationControlBase
from processors.landsat import QCProcessorLandsatMeta

class QCProcessorHarmonizationControlLandsat(QCProcessorHarmonizationControlBase, QCProcessorLandsatMeta):
    """Processor performing harmonization control for Landsat products."""
    pass
