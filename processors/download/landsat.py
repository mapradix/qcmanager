from processors.download.base import QCProcessorDownloadBase
from processors.search.landsat import QCConnectLandsat
from processors.landsat import QCProcessorLandsatMeta


class QCConnectLandsatDownload(QCConnectLandsat):
    def __init__(self, username, password, archive, backup_archive):
        from landsatxplore.earthexplorer import EarthExplorer

        self.api = EarthExplorer(
            username, password
        )

    def download_file(self, uuid, output_dir):
        from landsatxplore.util import guess_dataset
        from landsatxplore.earthexplorer import EE_DOWNLOAD_URL, DATASETS

        self.api.download(
            uuid, output_dir
        )

        # pseudo-checksum control
        dataset = guess_dataset(uuid)
        url = EE_DOWNLOAD_URL.format(dataset_id=DATASETS[dataset], scene_id=uuid)
        with self.api.session.get(url, stream=True, allow_redirects=True) as r:
            expected_filesize = int(r.headers['Content-Length'])

        return expected_filesize


class QCProcessorDownloadLandsat(QCProcessorDownloadBase, QCProcessorLandsatMeta):
    connector_class = QCConnectLandsatDownload

    def check_dependency(self):
        from landsatxplore.earthexplorer import EarthExplorer
