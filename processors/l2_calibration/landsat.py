import os
import glob
import subprocess

from shutil import copy2

from processors.l2_calibration.base import \
    QCProcessorL2CalibrationBase
from processors.landsat import QCProcessorLandsatMeta
from processors.exceptions import ProcessorFailedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorL2CalibrationLandsat(QCProcessorL2CalibrationBase, QCProcessorLandsatMeta):
    """Processor creating L2 products for Landsat."""

    def _get_ip_output_path(self, ip):
        return os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            ip.replace('LC08_L1', 'LC08_L2') + self.data_dir_suf
        )

    def calibrate(self, l1_data_dir, l2_data_dir):
        """Create L2 products."""
        manager_dir = os.getcwd()
        l1_product_dir = os.path.join(manager_dir, l1_data_dir)
        l2_product_dir = os.path.join(manager_dir, l2_data_dir)
        os.mkdir(l2_product_dir)

        try:
            self.run_calibration(l1_product_dir)
        except subprocess.CalledProcessError as e:
            Logger.error(
                "Calibration failed: {}".format(e)
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return {}

        self.transform_l2_to_tif(l1_product_dir, l2_product_dir)

        self.copy_kept_products(l1_product_dir, l2_product_dir)

        self.clean_l1_directory(l1_product_dir)

        return {}

    @staticmethod
    def transform_l2_to_tif(l1_product_dir, l2_product_dir):
        """Transform L2 products from native .img format to GeoTIIFs."""
        from osgeo import gdal

        regex = os.path.join(l1_product_dir, '*sr_band*img')
        for band in glob.glob(r'{}'.format(regex)):
            fn = os.path.split(band)[-1]
            new_fn = '{}.TIF'.format(os.path.splitext(fn)[0])
            band_tif = os.path.join(l2_product_dir, new_fn)
            gdal.Translate(band_tif, band)

    @staticmethod
    def run_calibration(l1_product_dir):
        """Create Landsat L2 products."""
        subprocess.run('convert_lpgs_to_espa --mtl *MTL.txt',
                       shell=True, check=True,
                       cwd=l1_product_dir)
        subprocess.run('do_lasrc_landsat.py --xml *xml',
                       shell=True, check=True,
                       cwd=l1_product_dir)

    @staticmethod
    def copy_kept_products(l1_product_dir, l2_product_dir):
        """Copy files created during calibration.

        From l1_product_dir into l2_product_dir.

        Should move files::
            LC08_L1TP_*_T1_MTL.txt
            LC08_L1TP_*_T1.xml
            LC08_L1TP_*_T1_ANG.txt
            LC08_L1TP_*_T1_sr_band1.img, *.band2 ... *.band7
            LC08_L1TP_*_T1_radsat_qa.img
            LC08_L1TP_*_T1_BQA.TIF
            LC08_L1TP_*_T1_bqa.img
        """
        regexes = ['*[tl]', '*BQA.TIF', '*qa.img']
        full_regexes = map(lambda r: os.path.join(l1_product_dir, r), regexes)

        for regex in full_regexes:
            for file in glob.glob(r'{}'.format(regex)):
                copy2(file, l2_product_dir)

    @staticmethod
    def clean_l1_directory(l1_product_dir):
        """Delete files created in l1_product_dir during the calibration."""
        regex = os.path.join(l1_product_dir, '*[grl]')
        for file in glob.glob(r'{}'.format(regex)):
            os.remove(file)

    def unarchive(self, filepath):
        """Unarchive a product.

        :param filepath: Path to the file to be unarchived.
        """
        dirname = os.path.join(
            os.path.dirname(filepath),
            os.path.basename(filepath).rstrip('.tar.gz')
        )

        if not os.path.exists(dirname):
            Logger.info("Unarchiving {}...".format(filepath))
            import tarfile
            try:
                with tarfile.open(filepath) as fd:
                    def is_within_directory(directory, target):
                        
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                    
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        
                        return prefix == abs_directory
                    
                    def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                    
                        for member in tar.getmembers():
                            member_path = os.path.join(path, member.name)
                            if not is_within_directory(path, member_path):
                                raise Exception("Attempted Path Traversal in Tar File")
                    
                        tar.extractall(path, members, numeric_owner=numeric_owner) 
                        
                    
                    safe_extract(fd, dirname)
            except (EOFError, tarfile.ReadError) as e:
                raise ProcessorFailedError(
                    self,
                    "broken {} - {}".format(filepath, e)
                )

        return dirname
