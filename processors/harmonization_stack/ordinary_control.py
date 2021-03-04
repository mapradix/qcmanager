import os
from abc import ABC, abstractmethod
from zipfile import ZipFile, BadZipFile

from processors import QCProcessorIPBase
from processors.exceptions import ProccessorDependencyError, \
    ProcessorRejectedError, ProcessorFailedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger
from manager import __version__


class QCProcessorOrdinaryControlStackBase(QCProcessorIPBase, ABC):
    """Ordinary control processor.
    """
    isMeasurementOf = "ordinaryControlMetric"
    isMeasurementOfSection = "harmonized"
    extension = '.zip'
    bands = None
    # check number of rows/cols (assuming 100x100km)
    tile_size = 100e3
    epsg = ('326', '327')
    img_format = ('GTiff',)

    def __init__(self, config, response):
        """Initialize the entire processor object.

        :param config: processor-related config file
        :param response: processor response managed by the manager
        """
        super(QCProcessorOrdinaryControlStackBase, self).__init__(
            config, response
        )

        # force output dir (requires defining _get_ip_output_path())
        self.output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )

    @staticmethod
    @abstractmethod
    def get_maximum_dtype():
        """Get maximal allowed data type."""
        pass

    def unarchive(self, filepath):
        """Unarchive a product.

        :param filepath: Path to the file to be unarchived.
        """
        with ZipFile(filepath) as fd:
            dirname = os.path.join(
                os.path.dirname(filepath), fd.namelist()[0]
            )
            try:
                fd.extractall(os.path.dirname(filepath))
            except BadZipFile as e:
                raise ProcessorFailedError(
                    self,
                    "broken {} - {}".format(
                        filepath, e
                    ))

        return dirname

    def check_metafile(self, filename):
        """Check metadata of a product.

        :param filename: file with metadata to be checked
        """
        try:
            from xml.etree import ElementTree
        except ImportError as e:
            raise ProccessorDependencyError(self, e)

        with open(filename, encoding='utf-8') as fd:
            # code inspired by
            # https://bitbucket.org/chchrsc/python-fmask/src/default/fmask/sen2meta.py
            root = ElementTree.fromstring(fd.read())
            nsPrefix = root.tag[:root.tag.index('}') + 1]
            nsDict = {'n1': nsPrefix[1:-1]}
            generalInfoNode = root.find('n1:General_Info', nsDict)

        return True if generalInfoNode is not None else False

    def check_dependency(self):
        from osgeo import gdal

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory
        """
        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
        }

        control_data = self._ordinary_control(meta_data['Tile Identifier'],
                                              output_dir=output_dir)

        # process primary product type
        response_data.update(control_data)

        return response_data

    def _ordinary_control(self, tile_id, output_dir=''):
        """Perform ordinary control.

        :param str tile_id: identificator of the tile
        :param str output_dir: path to the L2H dir
        """
        response_data = {}

        lh_title = os.path.split(output_dir)[-1]
        filename = os.path.join(output_dir, self.get_stack_name(lh_title))

        # 2. check if raster file is readable
        try:
            self.check_stack_read(filename)
        except ProcessorRejectedError:
            return response_data

        # 3. check if all bands are in the stack
        try:
            _ = self.check_bands(filename)
        except ProcessorRejectedError:
            return response_data

        level_key = 'harmonized'
        response_data[level_key] = {'tile': [{}]}
        response_data[level_key]['tile'][0]['id'] = tile_id
        response_data[level_key]['tile'][0]['raster'] = {}
        response_data[level_key]['tile'][0]['raster']['rastersComplete'] = True
        response_data[level_key]['tile'][0]['raster']['rastersRead'] = True

        lineage = 'http://qcmms.esa.int/QCMMS_QCManager_v2.0'
        response_data[level_key]['lineage'] = lineage

        # 4. read raster characteristics
        try:
            epsg_res, format_res, bits, rows, cols, res = \
                self.check_bands_properties(
                filename
            )
        except ProcessorRejectedError:
            return response_data

        # update response (epgs, format, bands)
        response_data[level_key]['tile'][0]['raster']['epsg'] = int(epsg_res)
        if format_res == 'GTiff':
            data_format = 'TIFF'
        else:
            data_format = '?'
        response_data[level_key]['tile'][0]['raster']['format'] = data_format
        response_data[level_key]['tile'][0]['raster']['bands'] = []
        for band in self.bands:
            response_data[level_key]['tile'][0]['raster']['bands'].append({
                'id': band
            })
        response_data[level_key]['tile'][0]['raster']['rows'] = rows
        response_data[level_key]['tile'][0]['raster']['cols'] = cols
        response_data[level_key]['tile'][0]['raster']['bits'] = bits
        response_data[level_key]['tile'][0]['raster']['resolution'] = res

        # 6. selected for next control
        qi_failed = []
        for attr in ('rastersComplete',):
            if not response_data[level_key]['tile'][0]['raster'][attr]:
                qi_failed.append(attr)
        response_data['value'] = len(qi_failed) < 1
        if qi_failed:
            Logger.error(
                "Rejected because of {}".format(','.join(qi_failed))
            )
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data

    @staticmethod
    def get_stack_name(lh_title):
        """Get filename in format stack_[lh_title].tif.

        :param lh_title: title of L2H product
        """
        return 'stack_{}.tif'.format(lh_title)

    def check_bands(self, filename):
        """Check the number of bands.

        :param filename: path to the stack
        """
        from osgeo import gdal
        stack = gdal.Open(filename)

        stack_len = stack.RasterCount
        intended_len = 6

        if not stack_len == intended_len:
            raise ProcessorRejectedError(
                self,
                'stack is supposed to contain {} bands, but contains {} '
                'instead'.format(intended_len, stack_len))

        stack = None

        return intended_len

    def check_stack_read(self, img_file):
        """Check if the stack is readable.

        :param img_file: path to the stack
        """
        from osgeo import gdal

        ds = gdal.Open(img_file)
        if ds is None:
            raise ProcessorRejectedError(
                self,
                "{} is not readable (invalid GDAL datasource)".format(
                    img_file
                ))

        ds = None
        Logger.debug("File {} is readable (valid GDAL datasource)".format(
            img_file
        ))

    def check_bands_properties(self, filename):
        """Check bands properties and their consistency throughout stack.

        :param filename: path to the stack
        """
        from osgeo import gdal, osr

        epsg_res = ''
        format_res = ''

        ds = gdal.Open(filename)
        nbands = ds.RasterCount
        Logger.debug("File {}: nbands={}".format(
            filename, nbands
        ))
        srs = osr.SpatialReference()
        srs.ImportFromWkt(ds.GetProjectionRef())
        epsg = srs.GetAuthorityCode(None)
        if not epsg:
            raise ProcessorRejectedError(
                self,
                "Unknown EPSG code ({})".format(
                    filename
                ))

        Logger.debug("File {}: srs={}".format(
            filename, epsg
        ))
        format_ds = ds.GetDriver().ShortName
        if not epsg_res:
            epsg_res = epsg
            format_res = format_ds
        else:
            if epsg_res != epsg or format_res != format_ds:
                raise ProcessorRejectedError(
                    self,
                    "Inconsistent EPSG code or data format ({})".format(
                        filename
                    ))

        # spatial resolution
        trans = ds.GetGeoTransform()
        xres = abs(trans[1])
        yres = abs(trans[5])

        band1 = ds.GetRasterBand(1)
        dtype = band1.DataType
        bits = 16 if dtype == 2 else 8
        rows = band1.YSize
        cols = band1.XSize

        # check data type
        if dtype > self.get_maximum_dtype():
            raise ProcessorRejectedError(
                self,
                "Unsupported data type"
            )

        # check consistency for all other bands
        for ib in range(2, len(self.bands) + 1):
            band = ds.GetRasterBand(ib)
            if band is None:
                raise ProcessorRejectedError(
                    self,
                    "Unable to read band {} from {}".format(
                        ib, filename
                    ))
            if band.YSize != rows:
                raise ProcessorRejectedError(
                    self,
                    "Band {} has unexpected nummber of rows. {} expected, "
                    "but found {} instead".format(ib, rows, band.YSize))
            if band.XSize != cols:
                raise ProcessorRejectedError(
                    self,
                    "Band {} has unexpected nummber of cols. {} expected, "
                    "but found {} instead".format(ib, cols, band.XSize))
            if band.DataType != dtype:
                raise ProcessorRejectedError(
                    self,
                    "Band {} has unexpected data type. {} expected, "
                    "but found {} instead".format(ib, dtype, band.DataType))

            tol = 20e3  # tolerance of 20km
            for dim in (rows * yres,
                        cols * xres):
                if dim > self.tile_size + tol or dim < self.tile_size - tol:
                    raise ProcessorRejectedError(
                        self,
                        "File {}: unexpected tile size ({}km)".format(
                            filename, dim / 1000
                        ))

        ds = None

        # check data format
        Logger.debug("Detected file format: {}".format(format_res))
        if format_res not in self.img_format:
            raise ProcessorRejectedError(
                self,
                "Invalid data format detected ({})".format(
                    format_res
                ))

        return epsg_res, format_res, bits, rows, cols, abs(xres)

    def _get_ip_output_path(self, ip):
        """Get processor's IP output path (SAFE dir).

        :param str ip: image product

        :return str: output path
        """
        return os.path.join(
            self.output_path,
            '{}.SAFE'.format(ip)
        )
