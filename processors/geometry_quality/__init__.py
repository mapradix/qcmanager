from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorGeometryQuality(QCProcessorMultiBase):
    """Geometry quality control processor [detailed control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.geometry_quality.sentinel import QCProcessorGeometryQualitySentinel
        return QCProcessorGeometryQualitySentinel

    def processor_landsat8(self):
        from processors.geometry_quality.landsat import QCProcessorGeometryQualityLandsat
        return QCProcessorGeometryQualityLandsat
