from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorCloudCoverage(QCProcessorMultiBase):
    """Cloud coverage control processor [defailed control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.cloud_coverage.sentinel import QCProcessorCloudCoverageSentinel
        return QCProcessorCloudCoverageSentinel

    def processor_landsat8(self):
        from processors.cloud_coverage.landsat import QCProcessorCloudCoverageLandsat
        return QCProcessorCloudCoverageLandsat
