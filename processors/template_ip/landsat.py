from processors.template_ip.base import QCProcessorTemplateIPBase
from processors.landsat import QCProcessorLandsatMeta

class QCProcessorTemplateIPLandsat(QCProcessorTemplateIPBase, QCProcessorLandsatMeta):
    def check_dependency(self):
        """Check processor's software dependecies."""
        pass

