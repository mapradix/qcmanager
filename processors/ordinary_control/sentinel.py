import os
from zipfile import ZipFile, BadZipFile

from processors.ordinary_control.base import QCProcessorOrdinaryControlBase
from processors.exceptions import ProcessorRejectedError
from processors.sentinel import QCProcessorSentinelMeta


class QCProcessorOrdinaryControlSentinel(QCProcessorOrdinaryControlBase, QCProcessorSentinelMeta):
    # check number of rows/cols (assuming 100x100km)
    tile_size = 100e3
    img_format = ('JP2OpenJPEG', 'JPEG2000')
    epsg = ('326', '327')
    mtd_files = { 'level1' : ('MTD_TL.xml', 'MTD_MSIL1C.xml'),
                  'level2' : ('MTD_TL.xml', 'MTD_MSIL2A.xml')
    }
    has_calibration_metadata = True

    def __init__(self, config, response):
        super(QCProcessorOrdinaryControlSentinel, self).__init__(
            config, response
        )

    def unarchive(self, filepath, dirname):
        try:
            with ZipFile(filepath) as fd:
                dirname = os.path.join(
                    os.path.dirname(filepath), fd.namelist()[0]
                )
                fd.extractall(os.path.dirname(filepath))
        except BadZipFile as e:
            raise ProcessorRejectedError(
                self,
                "broken {} - {}".format(filepath, e)
            )

        return dirname

    def check_epsg_tile_name(self, epsg_res, filepath):
        if 'T{}'.format(epsg_res[-2:]) not in os.path.basename(filepath):
            raise ProcessorRejectedError(
                self,
                "Inconsistency between EPSG code and Tile number detected"
            )

    def check_metafile(self, filename):
        try:
            from xml.etree import ElementTree
        except ImportError as e:
            raise ProccessorDependencyError(self, e)

        with open(filename, encoding='utf-8') as fd:
            # code inspired by
            # https://bitbucket.org/chchrsc/python-fmask/src/default/fmask/sen2meta.py
            root = ElementTree.fromstring(fd.read())
            nsPrefix = root.tag[:root.tag.index('}')+1]
            nsDict = {'n1':nsPrefix[1:-1]}
            generalInfoNode = root.find('n1:General_Info', nsDict)

        return True if generalInfoNode is not None else False

    def get_bands(self, level=1):
        """Return band identifiers to be used for regexes."""
        bands = list(map(lambda x: 'B{0:02d}'.format(x), range(1, 13))) + ['B8A']

        if level == 2 and self.extension == '.zip':
            bands.remove('B08')
            bands.remove('B10')

        return bands

    def get_lineage_level(self, filename):
        """Get lineage for the product and its level.

        :return: A string to be written as a product lineage
        """
        basename = os.path.basename(filename)
        n_version = basename.rstrip(self.extension).split('_')[3]
        return 'http://qcmms.esa.int/{}'.format(n_version)

    @staticmethod
    def get_maximum_dtype():
        return 2
