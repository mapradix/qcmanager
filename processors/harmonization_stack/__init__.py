from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorHarmonizationStack(QCProcessorMultiBase):
    """Processor creating a resampled stack from individual band files [multi-sensor control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.harmonization_stack.sentinel import QCProcessorHarmonizationStackSentinel
        return QCProcessorHarmonizationStackSentinel

    def processor_landsat8(self):
        from processors.harmonization_stack.landsat import QCProcessorHarmonizationStackLandsat
        return QCProcessorHarmonizationStackLandsat
