from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorHarmonizationControl(QCProcessorMultiBase):
    """Harmonization control processor [multi-sensor control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.harmonization_control.sentinel import QCProcessorHarmonizationControlSentinel
        return QCProcessorHarmonizationControlSentinel

    def processor_landsat8(self):
        from processors.harmonization_control.landsat import QCProcessorHarmonizationControlLandsat
        return QCProcessorHarmonizationControlLandsat
