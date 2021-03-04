#------------------------------------------
# author:    Lukas Brodsky
# copyright: Mapradix s.r.o.
# email:     lukas.brodsky@mapradix.cz
# status:    testing
# project:   QCMMS
# processor: 1: Image Products Access
# purpose:   Download S2 metdata, etc.
# site:      CZ - Prague
#------------------------------------------

from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorOrdinaryControl(QCProcessorMultiBase):
    """Ordinary control processor [ordinary control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.ordinary_control.sentinel import QCProcessorOrdinaryControlSentinel
        return QCProcessorOrdinaryControlSentinel

    def processor_landsat8(self):
        from processors.ordinary_control.landsat import QCProcessorOrdinaryControlLandsat
        return QCProcessorOrdinaryControlLandsat
