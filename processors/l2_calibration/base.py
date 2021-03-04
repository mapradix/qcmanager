import os
import shutil
from abc import ABC, abstractmethod

from manager.logger import Logger # must be called before processors.exceptions
from manager.logger.db import DbIpOperationStatus
from manager.io import JsonIO

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorFailedError


class QCProcessorL2CalibrationBase(QCProcessorIPBase, ABC):
    """Processor to create L2 products abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "L2Calibration"
    level2_data = False

    def __init__(self, config, response):
        super(QCProcessorL2CalibrationBase, self).__init__(
            config, response
        )

        # force output_dir (requires defining _get_ip_output_path())
        self.output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )

    def check_dependency(self):
        from osgeo import gdal

    @abstractmethod
    def calibrate(self, l1_data_dir, l2_data_dir):
        """Create L2 products.

        :param str l1_data-dir: input directory with level 1 products
        :param str l2_data-dir: output directory with level 1 products
        """
        pass

    @abstractmethod
    def _get_ip_output_path(self, ip):
        """Get image product output path.

        :param str ip: image product

        :return str: target path
        """
        pass

    @abstractmethod
    def unarchive(self, filepath):
        """Unarchive a product.

        :param filepath: Path to the file to be unarchived
        """
        pass

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory
        """
        response_data = {}

        metapath = os.path.join(
            self.config['project']['path'], self.config['project']['metapath']
        )
        json_file = os.path.join(metapath, meta_data['title'] + '.geojson')
        output_dir = self._get_ip_output_path(meta_data['title'])

        # check if L2 product is available
        try:
            title = meta_data['qcmms']['processing_level2']['title']
            l2_file = os.path.join(os.path.dirname(data_dir), title + self.extension)
            if os.path.exists(l2_file):
                Logger.debug(
                    'L2 product already downloaded ({}); no local calibration done'.format(
                        l2_file
                ))
                return response_data
        except KeyError:
            pass

        # unarchive L1C product if needed
        if self.data_dir_suf:
            filepath = data_dir.replace(self.data_dir_suf, self.extension)
        else:
            filepath = data_dir + self.extension

        try:
            _ = self.unarchive(filepath)
        except ProcessorFailedError:
            return response_data

        if not os.path.exists(output_dir) or \
           len(self.filter_files(output_dir, self.img_extension)) < 1:
            # no files found, do calibration
            Logger.debug(
                'L2 products will be created in {}'.format(output_dir)
            )

            if os.path.exists(output_dir):
                # remove broken product if available
                shutil.rmtree(output_dir)
            response_data.update(self.calibrate(data_dir, output_dir))
        else:
            Logger.debug("Level2 product found: {}".format(output_dir))

        # title to be written into metadata
        title = os.path.split(output_dir)[-1]
        if len(self.data_dir_suf) > 0:
            title = title.split(self.data_dir_suf)[0]

        meta_data.update({
            'qcmms': {
                'processing_level2': {
                    'title': title
                }
            }
        })
        JsonIO.write(json_file, meta_data)
        try:
            pass
        except ProcessorFailedError:
            self.set_response_status(DbIpOperationStatus.failed)

        return response_data
