from processors.template_ip.base import QCProcessorTemplateIPBase
from processors.sentinel import QCProcessorSentinelMeta

class QCProcessorTemplateIPSentinel(QCProcessorTemplateIPBase, QCProcessorSentinelMeta):
    def check_dependency(self):
        """Check processor's software dependecies."""
        pass

