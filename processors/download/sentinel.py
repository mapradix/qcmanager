from requests.exceptions import ConnectionError

from processors.download.base import QCProcessorDownloadBase, QCProcessorDownloadError
from processors.exceptions import ProcessorFailedError
from processors.search.sentinel import QCConnectSentinel
from processors.sentinel import QCProcessorSentinelMeta

from manager.logger import Logger


class QCConnectSentinelDownload(QCConnectSentinel):
    def download_file(self, uuid, output_dir):
        from sentinelsat.sentinel import SentinelAPIError, InvalidChecksumError

        try:
            self.api.download(
                uuid, output_dir, checksum=True
            )
        except (SentinelAPIError, ConnectionError, InvalidChecksumError) as e:
            # re-try with backup archive
            Logger.error("Unable to access {} ({}). Re-trying with {}...".format(
                self.archive, e, self.backup_archive
            ))
            self.api.api_url = self.backup_archive

            try:
                self.api.download(
                    uuid, output_dir, checksum=True
                )
            except (SentinelAPIError, ConnectionError, InvalidChecksumError) as e:
                raise QCProcessorDownloadError(e)


class QCProcessorDownloadSentinel(QCProcessorDownloadBase, QCProcessorSentinelMeta):
    connector_class = QCConnectSentinelDownload

    def check_dependency(self):
        from sentinelsat.sentinel import SentinelAPI
