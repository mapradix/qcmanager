import os
import re
from datetime import datetime
import shutil
from string import Template
from xml.etree import ElementTree
from abc import ABC, abstractmethod

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorCriticalError, ProccessorDependencyError, \
    ProcessorRejectedError

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger


class QCProcessorOrdinaryControlBase(QCProcessorIPBase, ABC):
    """Ordinary control processor abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "ordinaryControlMetric"

    def __init__(self, config, response):
        super(QCProcessorOrdinaryControlBase, self).__init__(
            config, response
        )

        # force output dir (requires defining _get_ip_output_path())
        self.output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )

    @abstractmethod
    def unarchive(self, filepath, dirname):
        """Unarchive image product.

        :param str filepath: archive file path
        :param str dirname: target directory
        """
        pass

    @abstractmethod
    def check_epsg_tile_name(self, epsg_res, filepath):
        """Check EPSG tile consistency.

        :param str epsg_res: EPSG code
        :param str filepath: file path to be checked
        """
        pass

    @abstractmethod
    def check_metafile(self, filename):
        """Check product metadata.

        :param str filename: file path to be check
        """
        pass

    @abstractmethod
    def get_bands(self, level=1):
        """Return band identifiers to be used for regexes.

        :param int level: level to be processed (1, 2)
        """
        pass

    @abstractmethod
    def get_lineage_level(self, filename):
        """Get lineage for the product and its level.

        :param str filename: file path for getting lineage

        :return: A string to be written as a product lineage
        """
        pass

    @staticmethod
    @abstractmethod
    def get_maximum_dtype():
        """Get data type.
        """
        pass

    def check_dependency(self):
        """Check procesor's software dependecies."""
        from osgeo import gdal

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.

        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            "generatedAtTime": datetime.now(),
            'value' : False
        }

        # process primary product type
        response_data.update(
            self._ordinary_control(
                self._get_file_path(meta_data['title'])
        ))

        # process level2 product type if defined
        level2_product = self.get_processing_level2(meta_data)
        if level2_product:
            response_data.update(
                self._ordinary_control(
                    self._get_file_path(level2_product['title']),
                    level=2
                )
            )
        else:
            Logger.error("Level2 product not found for {}".format(
                meta_data['title']
            ))
            response_data['value'] = False

        return response_data

    def _get_file_path(self, ip):
        """Get image product path.

        :param str ip: image product

        :return str: file path
        """
        return os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            ip + self.extension
        )

    def _ordinary_control(self, filepath, level=1):
        """Perform ordinary control.

        :param str filepath: path to zip file
        :param int level: level number for response
        """
        from osgeo import gdal, osr

        if level not in (1, 2):
            raise ProcessorCriticalError(
                self,
                "Unsupported level: {}".format(level)
            )

        response_data = {
            'value' : False
        }

        # 1. unarchive product
        dirname = os.path.join(
            os.path.dirname(filepath),
            os.path.basename(filepath).rstrip(self.extension) + self.data_dir_suf
        )
        try:
            if not os.path.exists(filepath) and \
               not os.path.exists(dirname):
                raise ProcessorRejectedError(
                    self,
                    "No input data: {}/{} not found".format(filepath, dirname)
                )
            if not os.path.exists(dirname):
                Logger.info("Unarchiving {}...".format(filepath))
                dirname = self.unarchive(filepath, dirname)
        except ProcessorRejectedError:
            return response_data

        # 2. check if all bands are available
        try:
            img_files, band_res = self.check_bands(dirname, level)
        except ProcessorRejectedError:
            return response_data

        level_key = 'level{}'.format(level)
        response_data[level_key] = {}
        response_data[level_key]['rastersComplete'] = True
        response_data[level_key]['channels'] = len(img_files)
        response_data[level_key]['lineage'] = self.get_lineage_level(filepath)

        # 3. check if raster file is readable
        try:
            self.check_bands_read(img_files)
        except ProcessorRejectedError:
            return response_data

        Logger.info("All imagery files found and readable")
        response_data[level_key]['rastersRead'.format(level)] = True

        # 4. read raster characteristics
        try:
            epsg_res, format_res = self.check_bands_properties(img_files, band_res)
            self.check_epsg_tile_name(epsg_res, filepath)
        except ProcessorRejectedError:
            return response_data

        # update response (epgs, format, bands)
        response_data[level_key]['epsg'] = int(epsg_res)
        if format_res == 'JP2OpenJPEG':
            data_format = 'JPEG'
        elif format_res == 'GTiff':
            data_format = 'TIFF'
        else:
            data_format = '?'
        response_data[level_key]['format'] = data_format
        response_data[level_key]['bands'] = band_res

        # 5. check metadata
        metadata_read = self.check_metadata(dirname, level)
        response_data[level_key]['metadataRead'] = metadata_read[0]
        if self.has_calibration_metadata:
            response_data[level_key]['calibrationMetadata'] = metadata_read[1]
        else:
            response_data[level_key]['calibrationMetadata'] = True

        # 6. selected for next control
        qi_failed = []
        for attr in ('rastersComplete',
                     'rastersRead',
                     'metadataRead',
                     'calibrationMetadata'):
            if not response_data[level_key][attr]:
                qi_failed.append(attr)
        response_data['value'] = len(qi_failed) < 1
        if qi_failed:
            Logger.error(
                "Rejected because of {}".format(','.join(qi_failed))
            )
            self.set_response_status(DbIpOperationStatus.rejected)

        return response_data

    def check_bands(self, dirname, level):
        """Check raster bands.

        :param str dirname: image product directory.
        :param int level: level to be checked (1, 2)

        :return tuple: image filenames, bands
        """
        img_files_all = self.filter_files(
            dirname, extension=self.img_extension
        )

        band_res = []
        img_files = []

        bands = self.get_bands(level)

        for band in bands:
            pattern = r'.*_{}.*{}'.format(band, self.img_extension)
            found = False
            for item in img_files_all:
                if not re.search(pattern, item):
                    continue
                found = True
                Logger.debug("File {} found: {}".format(
                    pattern, item
                ))
                # B10 or B10_10m, ...
                band_res.append(
                    { 'id': item[item.find(band):].split('.')[0] }
                )
                img_files.append(item)

            if not found:
                raise ProcessorRejectedError(
                    self,
                    "{} not found in {}".format(
                        pattern, dirname
                ))

        return img_files, band_res

    def check_bands_read(self, img_files):
        """Check if raster band can be read.

        Raise ProcessorRejectedError on failure.

        :param list img_files: list of image files
        """
        from osgeo import gdal

        for imfile in img_files:
            ds = gdal.Open(imfile)
            if ds is None:
                raise ProcessorRejectedError(
                    self,
                    "{} is not readable (invalid GDAL datasource)".format(
                        imfile
                ))

            ds = None
            Logger.debug("File {} is readable (valid GDAL datasource)".format(
                imfile
            ))

    def check_bands_properties(self, img_files, band_res):
        """Check band properties.

        Raise ProcessorRejectedError on failure.

        :param list img_files: list of image files
        :param dict band_res: band properties

        :return tuple: epgs, format properties
        """
        from osgeo import gdal, osr

        epsg_res = ''
        format_res = ''

        i = 0
        for imfile in img_files:
            ds = gdal.Open(imfile)
            nbands = ds.RasterCount
            Logger.debug("File {}: nbands={}".format(
                imfile, nbands
            ))
            srs = osr.SpatialReference()
            srs.ImportFromWkt(ds.GetProjectionRef())
            epsg = srs.GetAuthorityCode(None)
            if not epsg:
                raise ProcessorRejectedError(
                    self,
                    "Unknown EPSG code ({})".format(
                        imfile
                ))

            Logger.debug("File {}: srs={}".format(
                imfile, epsg
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
                            imfile
                    ))

            # spatial resolution
            trans = ds.GetGeoTransform()
            xres = abs(trans[1])
            yres = abs(trans[5])

            for ib in range(1, nbands+1):
                band = ds.GetRasterBand(ib)
                if band is None:
                    raise ProcessorRejectedError(
                        self,
                        "Unable to read band {} from {}".format(
                            ib, imfile
                    ))

                dtype = band.DataType
                Logger.debug("File {} (band {}): dtype={} rows={} cols={}".format(
                    imfile, ib, dtype, band.YSize, band.XSize
                ))
            band_res[i]['rows'] = band.YSize
            band_res[i]['cols'] = band.XSize
            tol = 20e3    # tolerance of 20km
            for dim in (band_res[i]['rows'] * yres,
                        band_res[i]['cols'] * xres):
                if dim > self.tile_size + tol or dim < self.tile_size - tol:
                    raise ProcessorRejectedError(
                        self,
                        "File {}: unexpected tile size ({}km)".format(
                            imfile, dim / 1000
                    ))

            # check data type
            if dtype > self.get_maximum_dtype():
                raise ProcessorRejectedError(
                    self,
                    "Unsupported data type"
                )

            band_res[i]['bits'] = 16 if dtype == 2 else 8
            band_res[i]['resolution'] = xres

            i += 1
            ds = None

        # check data format
        Logger.debug("Detected file format: {}".format(format_res))
        if format_res not in self.img_format:
            raise ProcessorRejectedError(
                self,
                "Invalid data format detected ({})".format(
                    format_res
            ))

        return epsg_res, format_res

    def check_metadata(self, dirname, level):
        """Check metadata.

        Raise ProcessorRejectedError on failure.

        :param str dirname: image product directory
        :param int level: level to be checked (1,2)

        return tuple: bool, bool
        """
        mtd_files = []
        for mtd in self.mtd_files['level{}'.format(level)]:
            mtd_files.append(
                self.filter_files(
                    dirname,
                    mtd
                )
            )

        if len(mtd_files) != len(self.mtd_files['level{}'.format(level)]):
            raise ProcessorRejectedError(
                self,
                "Metadata not complete ({} files found / {} files expected)".format(
                    len(mtd_files), len(self.mtd_files)
            ))

        metadata_read = [False] * len(self.mtd_files)
        idx = 0
        for files in mtd_files:
            if not files or len(files) > 1:
                return metadata_read

            filename = files[0]
            if self.check_metafile(filename):
                metadata_read[idx] = True
            else:
                metadata_read[idx] = False
            Logger.debug("File {} parsed successfully: {}".format(
                filename, metadata_read[idx]
            ))
            idx += 1

        return metadata_read

    def _get_ip_output_path(self, ip):
        """Get processor's IP output path (SAFE dir).

        :param str ip: image product

        :return str: output path
        """
        return os.path.join(
            self.output_path,
            '{}.SAFE'.format(ip)
        )
