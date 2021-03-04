from processors import QCProcessorMultiBase, identifier_from_file

class QCProcessorTemplateIP(QCProcessorMultiBase):
    """Template image product multi-sensor processor."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        """Sentinel-2 specific implementation.

        :return QCProcessorTemplateIPSentinel:
        """
        from processors.template_ip.sentinel import QCProcessorTemplateIPSentinel
        return QCProcessorTemplateIPSentinel

    def processor_landsat8(self):
        """Landsat-8 specific implementation.

        :return QCProcessorTemplateIPLandsat:
        """
        from processors.template_ip.landsat import QCProcessorTemplateIPLandsat
        return QCProcessorTemplateIPLandsat
    
