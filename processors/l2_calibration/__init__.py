from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorL2Calibration(QCProcessorMultiBase):
    """Processor to create L2 products [delivery control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.l2_calibration.sentinel import \
            QCProcessorL2CalibrationSentinel
        return QCProcessorL2CalibrationSentinel

    def processor_landsat8(self):
        from processors.l2_calibration.landsat import \
            QCProcessorL2CalibrationLandsat
        return QCProcessorL2CalibrationLandsat
