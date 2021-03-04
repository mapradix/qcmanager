import os
import tempfile
import subprocess

import shutil
from zipfile import ZipFile, BadZipFile

from processors.l2_calibration.base import \
    QCProcessorL2CalibrationBase
from processors.sentinel import QCProcessorSentinelMeta
from processors.exceptions import ProcessorFailedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorL2CalibrationSentinel(QCProcessorL2CalibrationBase, QCProcessorSentinelMeta):
    """Processor creating L2 products for Sentinel."""

    def __init__(self, config, response):
        super(QCProcessorL2CalibrationSentinel, self).__init__(
            config, response
        )

        self.temp_dir = os.path.join(
            tempfile.gettempdir(),
            '{}_{}'.format(self.__class__.__name__, os.getpid())
        )
        os.makedirs(self.temp_dir)

    def __del__(self):
        shutil.rmtree(self.temp_dir)
        Logger.debug("Directory {} removed".format(self.temp_dir))

    def _get_ip_output_path(self, ip):
        return os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            ip.replace('L1C', 'L2A') + self.data_dir_suf
        )

    def calibrate(self, l1_data_dir, l2_data_dir):
        """Create L2 products."""
        # temp_dir needed because use_cases contain resolution in their names
        # and sen2cor is not capable of working with output_dir with
        # a resolution in the name

        if self.config.has_section('sen2cor'):
            sen2cor_2_8 = '02.08' in self.config['sen2cor']['path']
            sen2cor_path = self.config['sen2cor']['path']
            if sen2cor_path[-1] != os.sep:
                sen2cor_path += os.sep
        else:
            sen2cor_2_8 = True
            sen2cor_path = ''

        output_path = os.path.join(self.temp_dir, os.path.basename(l1_data_dir))
        if sen2cor_2_8:
            # Sen2Cor 2.8
            # https://forum.step.esa.int/t/sen2cor-2-8-fails-on-product-from-early-2016-bool-object-has-no-attribute-spacecraft-name/16046
            sen2cor_string = '{}L2A_Process --resolution {} --cr_only ' \
                '--output_dir {} {}'.format(
                    sen2cor_path,
                    self.config['land_product']['geometric_resolution'],
                    output_path, os.path.abspath(l1_data_dir)
                )
        else:
            # Sen2Cor 2.5.5
            sen2cor_string = '{}{}L2A_Process' \
                ' --resolution {}' \
                ' --cr_only {}'.format(
                    self.config['sen2cor']['path'], os.path.sep,
                    self.config['land_product']['geometric_resolution'],
                    os.path.abspath(l1_data_dir)
                )

        Logger.debug("Running {}".format(sen2cor_string))
        try:
            subprocess.run(sen2cor_string, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            Logger.error(
                "Calibration failed: {}".format(e)
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return {}

        if sen2cor_2_8:
            # now move the L2 product from the temp dir to the right one
            sen2cor_dir = os.listdir(output_path)[0]
            source_path = os.path.join(output_path, sen2cor_dir)

            # do not use move() to avoid cross-link device error
            shutil.copytree(source_path, l2_data_dir)
            Logger.debug("Directory {} copied to {}".format(
                source_path, l2_data_dir
            ))

            Logger.debug("Image file test (source): {}".format(
                self.filter_files(source_path, extension=self.img_extension)[0]
            ))
            Logger.debug("Image file test (target): {}".format(
                self.filter_files(l2_data_dir, extension=self.img_extension)[0]
            ))

            # remove tmp output directory
            shutil.rmtree(output_path)
            Logger.debug("Directory {} removed".format(output_path))

        return {}

    def unarchive(self, filepath):
        """Unarchive a product.

        :param filepath: Path to the file to be unarchived.
        """
        dirname = os.path.join(
            os.path.dirname(filepath),
            os.path.basename(filepath).rstrip('.zip') + self.data_dir_suf
        )

        if not os.path.exists(dirname):
            Logger.info("Unarchiving {}...".format(filepath))
            try:
                with ZipFile(filepath) as fd:
                    dirname = os.path.join(
                        os.path.dirname(filepath), fd.namelist()[0]
                    )
                    fd.extractall(os.path.dirname(filepath))
            except BadZipFile as e:
                raise ProcessorFailedError(
                    self,
                    "broken {} - {}".format(filepath, e)
                )

        return dirname
