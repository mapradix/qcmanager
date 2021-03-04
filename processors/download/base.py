import os
from datetime import datetime
import json
from abc import ABC, abstractmethod

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorCriticalError

from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus
from manager.io import datetime_format


class QCProcessorDownloadError(Exception):
    """Download error."""
    pass

class QCProcessorDownloadBase(QCProcessorIPBase, ABC):
    """Download processor abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "deliveryControlMetric"

    def __init__(self, config, response):
        super(QCProcessorDownloadBase, self).__init__(
            config, response
        )

        # force output dir (requires defining set_output_path())
        self.output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )

    def connect(self):
        """Connect provider API.

        :return obj: connector class
        """
        pt = self.get_platform_type()
        return self.connector_class(
            self.config['{}_platform'.format(pt)]['username'],
            self.config['{}_platform'.format(pt)]['password'],
            self.config['image_products']['{}_mission_archive'.format(pt)],
            self.config['image_products'].get('{}_mission_backup_archive'.format(pt))
        )

    def _download_file(self, uuid, identifier, output_dir):
        """Download IP.

        :param str uuid: UUID
        :param str identifier: IP identifier (title)
        :param str output_dir: output directory
        """
        output_file = os.path.join(output_dir, identifier + self.extension)
        if os.path.exists(output_file):
            Logger.debug("Data already downloaded. Skiping")
            return

        Logger.info("Downloading {} -> {}".format(
            uuid, identifier)
        )
        expected_filesize = self.connector.download_file(uuid, output_dir)

        # control downloaded file
        if self._delivery_control(output_file, expected_filesize):
            Logger.debug("Filename {} passed delivery control".format(
                output_file
            ))

    def _delivery_control(self, filename, expected_filesize=None):
        """Performs delivery control.

        * check if download file exists
        * check file size when expected value given

        Raise QCProcessorDownloadError on failure

        :param str filename: filepath to check
        :param int expected_filesize: expected file size in bytes or None
        """
        # check if file exists
        if not os.path.exists(filename):
            raise QCProcessorDownloadError(
                "File {} doesn't exist".format(filename
            ))

        if expected_filesize:
            # check expected filesize if given
            filesize = os.path.getsize(filename)
            if filesize != expected_filesize:
                raise QCProcessorDownloadError(
                    "File {} size ({}) differs from expected value ({})".format(
                        filename, filesize, expected_filesize
                    ))
            Logger.debug("File {} expected filesize check passed".format(
                filename
            ))

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        self.connector = self.connect()

        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            "generatedAtTime": datetime.now(),
            'status': 'IN_PROGRESS'
        }
        try:
            self._download_file(
                meta_data['id'], meta_data['title'],
                output_dir
            )
            downfile = os.path.join(
                output_dir,
                meta_data['title'] + self.extension
            )
            response_data['status'] = 'FINISHED'
            response_data['complete'] = True
            response_data['date'] = datetime_format(
                self.file_timestamp(downfile)
            )
            response_data['value'] = True

            # download level2 product
            level2_product = self.get_processing_level2(meta_data)
            if level2_product:
                self._download_file(
                    level2_product['id'],
                    level2_product['title'],
                    output_dir
                )
        except QCProcessorDownloadError as e:
            Logger.error("Unable to download {}: {}".format(
                meta_data['id'], e
            ))

            response_data['status'] = 'NOT_AVAILABLE'
            response_data['complete'] = False
            response_data['value'] = False
            self.set_response_status(DbIpOperationStatus.failed)

        return response_data

    def _get_ip_output_path(self, ip):
        """Get processor's IP output path (zip file).

        :param str ip: image product

        :return str: output path
        """
        return os.path.join(
            self.output_path,
            '{}{}'.format(ip, self.extension)
        )
