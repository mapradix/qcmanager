from manager.logger import Logger

from processors import QCProcessorMultiBase, identifier_from_file


class QCProcessorDownload(QCProcessorMultiBase):
    """Download processor [delivery control]."""
    identifier = identifier_from_file(__file__)

    def processor_sentinel2(self):
        from processors.download.sentinel import QCProcessorDownloadSentinel
        return QCProcessorDownloadSentinel

    def processor_landsat8(self):
        from processors.download.landsat import QCProcessorDownloadLandsat
        return QCProcessorDownloadLandsat
