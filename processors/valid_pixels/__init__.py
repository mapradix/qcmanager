from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorValidPixels(QCProcessorMultiBase):
    """Validity pixel control processor [detailed control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.valid_pixels.sentinel import QCProcessorValidPixelsSentinel
        return QCProcessorValidPixelsSentinel

    def processor_landsat8(self):
        from processors.valid_pixels.landsat import QCProcessorValidPixelsLandsat
        return QCProcessorValidPixelsLandsat
