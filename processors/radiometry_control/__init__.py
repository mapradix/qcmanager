from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorRadiometryControl(QCProcessorMultiBase):
    """Radiometry control processor [detailed control]."""    
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.radiometry_control.sentinel import QCProcessorRadiometryControlSentinel
        return QCProcessorRadiometryControlSentinel

    def processor_landsat8(self):
        from processors.radiometry_control.landsat import QCProcessorRadiometryControlLandsat
        return QCProcessorRadiometryControlLandsat
