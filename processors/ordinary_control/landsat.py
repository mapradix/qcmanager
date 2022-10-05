import os
import tarfile

from processors.ordinary_control.base import QCProcessorOrdinaryControlBase
from processors.landsat import QCProcessorLandsatMeta
from processors.exceptions import ProcessorRejectedError


class QCProcessorOrdinaryControlLandsat(QCProcessorOrdinaryControlBase, QCProcessorLandsatMeta):
    # check number of rows/cols (assuming 230x230km)
    tile_size = 230e3
    img_format = ('GTiff', 'ENVI')
    epsg = ('326', '327')
    mtd_files = {'level1': ('MTL.txt', ), 'level2': ('MTL.txt', )}
    has_calibration_metadata = False

    def __init__(self, config, response):
        super(QCProcessorOrdinaryControlLandsat, self).__init__(
            config, response
        )

    def unarchive(self, filepath, dirname):
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
            raise ProcessorRejectedError(
                self,
                "broken {} - {}".format(filepath, e)
            )

        return dirname

    def check_epsg_tile_name(self, epsg_res, filepath):
        pass

    def check_metafile(self, filename):
        element = None

        if 'txt' in filename:
            with open(filename) as fd:
                line = fd.readline().rstrip(os.linesep)
                element = line.endswith('L1_METADATA_FILE')
        else:
            try:
                from xml.etree import ElementTree as ET
            except ImportError as e:
                raise ProccessorDependencyError(self, e)

            root = ET.parse(filename)
            prefix = root.tag[:root.tag.index('}') + 1]

            needed_tag = '{}lpgs_metadata_file'.format(prefix)

            for i in root.iter():
                if i.tag == needed_tag:
                    element = i.text
                    break

        return True if element is not None else False

    def get_bands(self, level=1):
        """Return band identifiers to be used for regexes."""
        if level == 1:
            bands = list(map(lambda x: 'B{}'.format(x), range(1, 11)))
        else:
            bands = list(map(lambda x: 'sr_band{0:01d}'.format(x), range(1, 8)))
        return bands

    def get_lineage_level(self, filename):
        """Get lineage for the product and its level.

        :return: A string to be written as a product lineage
        """
        return 'http://qcmms.esa.int/lasrc'

    @staticmethod
    def get_maximum_dtype():
        return 3
