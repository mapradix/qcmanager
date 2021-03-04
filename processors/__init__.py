import os
import time
import copy
import re
import datetime
import shutil
import glob
from abc import ABC, abstractmethod
from enum import Enum

from processors.exceptions import ProcessorCriticalError,\
    ProcessorFailedError, ProcessorRejectedError, ProccessorDependencyError

from manager.response import QCResponse
from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus
from manager.io import JsonIO
from manager import __version__

from styles import StyleReader


class QCPlatformType(Enum):
    """Platform type.

    - primary
    - supplementary
    - unknown
    """
    Primary = 1
    Supplementary = 2
    Unknown = 0


class QCProcessorBase(ABC):
    """Processor base (abstract) class.

    :param config: processor-related config file
    :param QCResponse response: processor QI metadata response managed by the manager
    """
    def __init__(self, config, response):
        self.config = config

        # initialize result
        self._result = {'qi.files': {}}
        self.reset()

        # manager response
        self._response = response

        # response-related
        self._current_response_idx = -1
        self._measurement_prefix = "http://qcmms.esa.int/quality-indicators"

        # primary or suplementary
        self.platform_type = QCPlatformType.Unknown

    def set_identifier(self, identifier):
        """Set processor identifier.

        :param str: processor identifier
        """
        self.identifier = identifier
        Logger.info(
            "QCProcessor{} config started".format(self.identifier.capitalize())
        )

    def set_results(self):
        """Set processor results dictionary object.
        """
        for key, value in self._result['qi.files'].items():
            value = '{}_{}'.format(self.identifier, value)
            self._result['qi.files'][key] = value

    def _run_start(self):
        """Log start timestamp."""
        self._start = time.time()

    def _run_done(self):
        """Log end timestamp."""
        # finished
        Logger.info("{0} FINISHED in {1:.6f} sec".format(
            self.__class__.__name__, time.time() - self._start)
        )

    def run(self):
        """Perform processor tasks.
        """
        # log start computation
        self._run_start()

        # run computation
        response_data = self._run()

        # update response
        if response_data:
            self.update_response(response_data)

        # log computation finished
        self._run_done()

    def reset(self):
        """Reset processor results (number of processed image products)."""
        for status in DbIpOperationStatus:
            self._result['ip_operation.{}'.format(status.name)] = 0

    @staticmethod
    def filter_files(dirname, extension='.jp2', pattern=None):
        """Get files by a filter.

        :param str dirname: directory
        :param str extension: filter by extension
        :param str pattern: filter by pattern

        :return list: list of found files
        """
        if pattern:
            _pattern = re.compile(r'{}'.format(pattern))
        else:
            _pattern = re.compile(r'.*{}$'.format(extension))
        files = []
        for rec in os.walk(dirname):
            if not rec[-1]:
                continue

            match = filter(_pattern.match, rec[-1])
            if match is None:
                continue

            for f in match:
                files.append(os.path.join(rec[0], f))

        return files

    def previous(self):
        """Get previous processor name defined in the queue.

        :return str: processor identifier
        """
        try:
            idx = self.config['processors'].index(self.identifier)
        except ValueError:
            idx = -1
        if idx < 1:
            raise ProcessorCriticalError(
                self,
                "no previous processor defined"
            )

        return self.config['processors'][idx-1]

    def result(self):
        """Check processor results.

        Raise ProcessorResultError if not defined

        :return dict: result object (directory)
        """
        if not self._result:
            raise ProcessorResultError(self)

        return self._result

    def get_response(self):
        """Get processor response stack (QI metadata)

        :return list: processor's response (list of QI metadata collections)
        """
        return self._response

    def get_meta_data(self, ip):
        """Get provider-based metadata for given image product.

        :param str ip: image product
        
        :return dict: metadata
        """
        return JsonIO.read(
            os.path.join(
                self.config['project']['path'],
                self.config['project']['metapath'],
                ip + ".geojson"
        ))

    def get_data_dir(self, ip=None):
        """Get data directory.

        :param str ip: image product or None

        :return str: path
        """
        if ip and self.level2_data:
            ip = self.get_processing_level2(self.get_meta_data(ip))['title']

        down_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath']
        )
        if ip is None:
            return down_path
        
        return os.path.join(
            down_path,
            ip + self.data_dir_suf
        )

    @staticmethod
    def tif2jpg(tif_file, jpeg2000=False):
        """Convert GeoTIFF file to JPEG/JPEG2000.

        Note: GDAL doesn't allow to create JPEG/JPEG2000 directly, see
        https://gis.stackexchange.com/questions/270804/creating-jp2-with-gdal-in-python

        :param str tif_file: input GeoTIFF file
        :param jpeg2000: True to produce JPG2000 format

        :return str: output JPEG/JPEG2000 file
        """
        from osgeo import gdal

        # read input
        src = gdal.Open(tif_file)

        # define output options
        kwargs = {}
        if jpeg2000:
            kwargs['format'] = 'JP2OpenJPEG'
        else:
            # don't produce aux files
            # gdal.SetConfigOption('GDAL_PAM_ENABLED', 'NO')

            kwargs['format'] = 'JPEG'
            if src.GetRasterBand(1).GetRasterColorTable():
                kwargs['rgbExpand'] ='rgb'
        options = gdal.TranslateOptions(**kwargs)

        # write output
        jpg_file = os.path.splitext(tif_file)[0] + ".{}".format(
            'jp2' if jpeg2000 else 'jpg')
        tgt = gdal.Translate(
            jpg_file, src,
            options=options
        )
        tgt = None

        Logger.debug("Output response file {} created".format(jpg_file))

        return jpg_file

    @staticmethod
    def compute_value_count(filename):
        """Compute value count for input raster file.

        Raise ProcessorCriticalError on failure.

        :param str filename: raster filename
        
        :return tuple: value, count, ncells
        """
        from osgeo import gdal, gdalconst
        import numpy as np
        try:
            ds = gdal.Open(filename, gdalconst.GA_ReadOnly)
            band = ds.GetRasterBand(1)
            array = np.array(band.ReadAsArray())
            value, count = np.unique(array, return_counts=True)
            ncells = band.XSize * band.YSize
            if sum(count) != ncells:
                raise ProcessorFailedError(
                    self,
                    "File {}: cell number mismatch".format(filename)
                )
            ds = None
        except RuntimeError as e:
            raise ProcessorCriticalError(
                self,
                "Computing value/count statistics failed: {}".format(e)
            )

        return value, count, ncells
    
    def file_basename(self, filepath):
        """Return file basename.

        :param str filepath: path to the file

        :return str: file basename
        """
        base_path = os.path.abspath(filepath)[
            len(os.path.abspath(self.config['project']['path']))+1:
        ]
        tuc_name = self.config['catalog']['ip_parent_identifier'].split(':')[-1]

        # determine URL for catalog
        url = base_path
        if self.config.has_section('catalog') and \
           self.config['catalog'].get('response_url'):
            url = '{}/{}/{}'.format(
                self.config['catalog']['response_url'].rstrip('/'),
                tuc_name,
                base_path
            )

        # copy file to www directory if defined
        www_dir = self.config['catalog'].get('www_dir')
        if www_dir:
            target = os.path.join(
                www_dir,
                tuc_name,
                base_path.lstrip('/')
            )
            target_dir = os.path.dirname(target)
            if not os.path.exists(target_dir):
                os.makedirs(target_dir)
            shutil.copyfile(
                filepath,
                target,
            )
            Logger.debug("File {} copied to {}".format(
                filepath, target
            ))

        return url


    @staticmethod
    def create_dir(dirname):
        """Create directory if not exists.

        :param dirname: directory to create
        """
        if not os.path.exists(dirname):
            os.makedirs(dirname)

    def get_processing_level2(self, data):
        """Get related level2 product IP.

        :param dict data: metafile data

        :return dict: level2 metadata
        """
        pt = self.get_platform_type()
        if not self.config['image_products'].get('{}_processing_level2'.format(pt)):
            Logger.debug("Level2 product type not enabled ({} missing)".format(
                '{}_processing_level2'.format(pt)
            ))
            return None

        try:
            level2_product = data['qcmms']['processing_level2']
        except KeyError:
            Logger.debug("Level2 product not found")
            return None

        Logger.debug("Level2 product found: {}".format(
            level2_product['title']
        ))

        return level2_product

    def get_last_ip_status(self, ip, status):
        """Get status for last image products.

        :param str ip: image product
        :param DbIpOperationStatus status: status

        :return DbIpOperationStatus: image product status
        """
        ip_status = status
        if status == DbIpOperationStatus.unchanged:
            # check also status of last IP processed by this processor
            ip_status = Logger.db_handler().processed_ip_status(self.identifier, ip)

        return ip_status

    @staticmethod
    def get_lineage():
        """Get lineage attribute.

        :return str: lineage
        """
        return 'http://qcmms.esa.int/QCMMS_QCManager_v{}'.format(
            __version__
        )

    def update_response(self, response_data):
        """Update current IP QI metadata response.

        :param dict response_data: key value pairs to update
        """
        if response_data.get('lineage') is None:
            # set default lineage if not defined
            response_data["lineage"] = self.get_lineage()

        Logger.debug("Updating response {} on idx {}".format(
            self.isMeasurementOf, self._current_response_idx
        ))
        try:
            response = self._response[self._current_response_idx]
        except (IndexError, KeyError):
            raise ProcessorCriticalError(
                self,
                "Inconsistence response counter: index ({}) length ({})".format(
                    self._current_response_idx, len(self._response)
                ))
        response.update(
            response_data,
            self.isMeasurementOf
        )

        # check metadata-response consistency
        ## 1. value == False -> rejected
        if response.get_value(self.isMeasurementOf) is False and \
           self.get_response_status() not in (DbIpOperationStatus.rejected, DbIpOperationStatus.failed):
            Logger.warning(
                "Value response status incosistency: setting status from {} to rejected".format(
                    self.get_response_status()
            ))
            # value is False -> switch to rejected status
            self.set_response_status(DbIpOperationStatus.rejected)
        ## 2. rejected | failed -> value = False
        if response.status in (DbIpOperationStatus.rejected, DbIpOperationStatus.failed) and \
           response.get_value(self.isMeasurementOf) is not False:
            Logger.warning(
                "Value response status inconsistency: setting value to False"
            )
            response.set_value(self.isMeasurementOf, False)

    def add_response(self, source_file):
        """Add a new QI metadata response.

        :param str source_file: source file (JSON metadata)
        """
        self._response.append(
            QCResponse(source_file)
        )

    def get_platform_type(self):
        """Get platform type

        :return str: product type
        """
        return self.platform_type.name.lower()

    @staticmethod
    def file_timestamp(filepath):
        """Get file timestamp.

        :param str filepath: file path
        """
        return datetime.datetime.fromtimestamp(os.stat(filepath).st_mtime)

    def set_response_counter(self, counter):
        """Set response counter (before running run())

        :param int counter: counter value
        """
        self._current_response_idx = counter

    def set_response_status(self, status):
        """Set status for current IP response.

        :param DbIpOperationStatus status: status to be set
        """
        try:
            self._response[self._current_response_idx].status = status
        except (IndexError, KeyError):
            raise ProcessorCriticalError(
                self,
                "Configuration inconsistency - current response index {} "
                "vs. number of responses {}".format(
                    self._current_response_idx, len(self._response)
            ))

    def get_response_status(self):
        """Get status of current IP response.

        :return DbIpOperationStatus status: IP status
        """
        try:
            return self._response[self._current_response_idx].status
        except (IndexError, KeyError):
            raise ProcessorCriticalError(
                self,
                "Configuration inconsistency - current response index {} "
                "vs. number of responses {}".format(
                    self._current_response_idx, len(self._response)
            ))

    def read_aoi(self, aoi):
        """Read AOI from GeoJSON file or directly from WKT.

        :param aoi: area of interest (WKT)

        :return str: WKT string
        """
        if re.search('Polygon\s*(.*)', aoi, re.IGNORECASE):
            # Fedeo is very pendatic, polygon must be uppercase
            return aoi.upper().replace('POLYGON ', 'POLYGON')

        try:
            # could be replaced by geojson + shapely
            from sentinelsat.sentinel import geojson_to_wkt, read_geojson
        except ImportError as e:
            Logger.critical("{} processor: {}".format(
                self.identifier, e)
            )
            return None

        # GeoJSON
        return geojson_to_wkt(
            read_geojson(aoi)
        )

class QCProcessorIPBase(QCProcessorBase):
    """Processor image product base class.

    :param config: processor-related config file
    :param QCResponse response: processor QI metadata response managed by the manager
    """
    def __init__(self, config, response):
        super(QCProcessorIPBase, self).__init__(config, response)

        # output path not defined
        self.output_path = None

        try:
            self.check_dependency()
        except ImportError as e:
            raise ProccessorDependencyError(self, e)

    @abstractmethod
    def check_dependency(self):
        """Check dependicies.

        Raise ProcessorDependencyError of failure.
        """
        pass

    def run(self):
        """Run processor tasks.

        :return int: response counter value
        """
        # log start computation
        self._run_start()

        # loop through image products (IP)
        processor_previous = self.previous()
        processed_ips = Logger.db_handler().processed_ips(
            processor_previous,
            platform_type=self.platform_type
        )
        ip_count = len(processed_ips)
        if ip_count < 1:
            Logger.warning("No IP products to process (previous processor: {})".format(
                processor_previous
            ))
        counter = 1
        for ip, status in processed_ips:
            # increment counter
            self._current_response_idx += 1

            Logger.info("({}) Processing {}... ({}/{})".format(
                self.identifier, ip, counter, ip_count
            ))
            counter += 1

            # get last IP status
            ip_status = self.get_last_ip_status(ip, status)

            # skip rejected IP (QA not passed)
            if ip_status == DbIpOperationStatus.rejected:
                self.ip_operation(ip, ip_status)
                response_data = self.get_last_response(ip)
                if response_data:
                    self.update_response(response_data)
                continue

            # set current response status from DB
            self.set_response_status(status)

            # read metadata
            meta_data = self.get_meta_data(ip)

            # define output path
            # check whether results exists
            if self.output_path is None:
                # output path not defined, assuming QI results (level2)
                try:
                    output_path = self._get_qi_results_path(
                        self.get_processing_level2(meta_data)['title']
                    )
                except TypeError:
                    Logger.warning("Level2 product not found, switching back to level1!")
                    output_path = self._get_qi_results_path(
                        meta_data['title']
                    )
                results_exist = self.check_qi_results(output_path)
            else:
                output_path = self.output_path
                results_exist = os.path.exists(self._get_ip_output_path(ip))

            # force absolute path
            try:
                output_path = os.path.abspath(output_path)
            except TypeError:
                raise ProcessorCriticalError(
                    self, "Output directory not defined!"
                )

            # determine whether to force the computation
            # ip_status is None -> no previous processor run detected
            force = status == DbIpOperationStatus.forced or \
                ip_status is None or \
                status == DbIpOperationStatus.unchanged and not results_exist

            # perform processor operations if requested
            if status in (DbIpOperationStatus.added, DbIpOperationStatus.updated,
                          DbIpOperationStatus.failed) or force:
                if force:
                    # change status from unchanged to updated
                    if not results_exist:
                        Logger.debug("Missing results")
                    Logger.debug("Operation forced")

                # create processor result directory if not exists
                if output_path and not os.path.exists(output_path):
                    os.makedirs(output_path)

                # run processor computation if requested
                down_path = self.get_data_dir()
                if self.level2_data:
                    try:
                        ip_dd = self.get_processing_level2(meta_data)['title']
                    except TypeError:
                        # switch back to L1
                        ip_dd = ip
                else:
                    ip_dd = ip
                data_dir = os.path.join(
                    down_path,
                    '{}{}'.format(ip_dd, self.data_dir_suf
                ))
                Logger.debug("Data dir: {}".format(data_dir))
                Logger.debug("Output dir: {}".format(output_path))

                # run computation
                response_data = self._run(
                    meta_data, data_dir, output_path
                )
            else:
                # no change, get response data from previous run
                response_data = self.get_last_response(ip)

            # update response
            if response_data:
                self.update_response(response_data)

            # log IP operation
            self.ip_operation(
                ip,
                self._response[self._current_response_idx].status
            )

        # log computation finished
        self._run_done()

        return self._current_response_idx

    def ip_operation(self, ip, status, timestamp=None):
        """Log IP operation.

        :param str ip: image product
        :param DbIpOperationStatus status: status
        :param datetime: timestamp or None
        """
        self._result['ip_operation.{}'.format(status.name)] += 1
        Logger.ip_operation(
            "{} processor: {} IP operation completed ({})".format(
                self.identifier, ip, status.name),
            identifier=self.identifier,
            ip=ip,
            timestamp=timestamp,
            status=status.value,
            platform_type=self.platform_type.value
        )

    def add_qi_result(self, key, postfix):
        """Add QI processor result.

        :param str key: key name
        :param str postfix: postfix filename

        :return str: return full path to a file
        """
        self._result['qi.files'][key] = postfix

    def get_qi_results(self, directory):
        """Get QI processor results.

        :param str directory: directory with QI files

        :return list: found QI files
        """
        return glob.glob('{}/{}*'.format(
            directory, self.identifier
        ))

    def check_qi_results(self, directory):
        """Check if number of resultant files is correct.

        :param str directory: directory to check

        :return bool: True if number of files is correct otherwise False
        """
        qi_files = self.get_qi_results(directory)

        # force absolute path
        for key, value in self._result['qi.files'].items():
            self._result['qi.files'][key] = os.path.join(directory, os.path.basename(value))

        nfound = 0
        for filename in self._result['qi.files'].values():
            if filename in qi_files:
                nfound += 1
        Logger.debug("Number of QI result files: {} (found) / {} (expected)".format(
            nfound, len(self._result['qi.files'].keys())
        ))

        return nfound == len(self._result['qi.files'].keys())

    def delete_qi_results(self, directory):
        """Try to delete QI results produced by processor.

        On strict mode raise error when object does not exist.

        :param str directory: directory where to search for QI results
        """
        if self.config['strict']['enabled'] and \
           not check_qi_results(directory):
            raise ProcessorCriticalError(
                self,
                "QI results mismatch detected"
            )

        for filepath in self.get_qi_results(directory):
            os.remove(filepath)

    def delete_path(self, path):
        """Try to delete a path object (file or directory).

        On strict mode raise error when object does not exist.

        :param str path: object to delete
        """
        try:
            if os.path.isfile(path):
                os.remove(path)
            else:
                shutil.rmtree(path)
        except FileNotFoundError as e:
            # raise critical error only in strict mode
            if self.config['strict']['enabled']:
                raise ProcessorCriticalError(self, '{}'.format(e))

    def _get_ip_output_path(self, ip):
        """Get processor's IP output path.

        :param str ip: image product

        :return str: output path
        """
        return self.output_path

    def _get_qi_results_path(self, ip):
        """Get IP specific QI results path.

        :param str ip: image product

        :return str: output path
        """
        # no output path defined, assuming QI results
        output_path = os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            ip + self.data_dir_suf,
        )
        if not os.path.exists(output_path):
            # no output directory defined
            Logger.debug("Output path {} does not exist".format(output_path))
            return None

        dirs = os.listdir(output_path)
        if 'GRANULE' in dirs:
            # only one directory is expected here (sentinel-2)
            dirs = os.listdir(os.path.join(output_path, 'GRANULE'))
            if len(dirs) != 1:
                raise ProcessorCriticalError(
                    "Unexpected number of data sub-directories"
                )

            return os.path.join(
                output_path,
                'GRANULE',
                dirs[0],
                'QI_DATA',
                'QCMMS'
            )

        return os.path.join(
            output_path,
            'QI_DATA',
            'QCMMS'
        )

    def get_last_response(self, ip, full=False):
        """
        Get QI metadata response from previous job.

        :param str ip: image product
        :param bool full: True for full data otherwise only relevant part

        :return dict: QI metadata
        """
        try:
            job_id = Logger.db_handler().last_job_id(self.config['processors'][0])
        except KeyError:
            raise ProcessorCriticalError(self, "No processors defined in config")

        if not job_id:
            Logger.debug("First run? Unable to get last response from JSON file")
            return None

        json_file = os.path.join(
            self.config['logging']['directory'],
            '{0:05d}'.format(job_id),
            ip + '.json')

        if not os.path.exists(json_file):
            raise ProcessorCriticalError(
                self,
                "Response file {} not found".format(
                    json_file
            ))

        data = JsonIO.read(json_file, response=True)
        if full:
            return data

        relevant_part = QCResponse(data).get(
            self.isMeasurementOf
        )
        if not relevant_part:
            if self.config['strict']['enabled']:
                raise ProcessorCriticalError(
                    self,
                    "Unable to get relevant part for {} ({})".format(
                        self.isMeasurementOf, ip
                ))
            else:
                return {}
        if hasattr(self, "isMeasurementOfSection"):
            relevant_part_tmp = {}
            for key in relevant_part.keys():
                if key in ("isMeasurementOf",
                           "value",
                           "lineage",
                           self.isMeasurementOfSection):
                    relevant_part_tmp[key] = relevant_part[key]
            relevant_part = relevant_part_tmp

        return relevant_part

    def set_platform_type(self, ptype):
        """Set current platform type.

        :param str: QCPlatformType string
        """
        if ptype == 'primary':
            pptype = QCPlatformType.Primary
        elif ptype == 'supplementary':
            pptype = QCPlatformType.Supplementary
        else:
            raise ProcessorCriticalError(
                self,
                "Unsupported platform type: {}".format(ptype)
            )

        self.platform_type = pptype

    def resample_output(self, input_file, output_file, resolution=10, overwrite=True):
        """Resample raster into target resolution.

        :param str input_file: raster file to resample
        :param str output_file: output resampled GTIFF raster file
        :param int resolution: target resolution
        :param bool overwrite: True to overwrite existing files

        :return str: path to resampled JPG response file
        """
        from osgeo import gdal, gdalconst

        if input_file == output_file or (os.path.exists(output_file) and not overwrite):
            # no resampling needed, just return output file
            return self.tif2jpg(output_file)

        try:
            # open input data
            ids = gdal.Open(input_file, gdalconst.GA_ReadOnly)
            iproj = ids.GetProjection()
            itrans = ids.GetGeoTransform()
            band_ref = ids.GetRasterBand(1)
            res_coef = abs(int(itrans[1] / resolution)) # assuming xres == yres

            # open output data
            driver= gdal.GetDriverByName('GTiff')
            ods = driver.Create(output_file,
                                band_ref.XSize * res_coef, band_ref.YSize * res_coef,
                                eType=band_ref.DataType)
            otrans = list(copy.copy(itrans))
            otrans[1] /= res_coef
            otrans[5] /= res_coef
            ods.SetGeoTransform(otrans)
            ods.SetProjection(iproj)

            # reproject
            gdal.ReprojectImage(ids, ods, iproj, iproj, gdalconst.GRA_Bilinear)
            band_out = ods.GetRasterBand(1)
            if band_ref.GetNoDataValue() is None:
                band_out.SetNoDataValue(0.0)
            else:
                band_out.SetNoDataValue(band_ref.GetNoDataValue())
            band_out.SetDefaultRAT(band_ref.GetDefaultRAT())

            # set color table
            StyleReader(self.identifier).set_band_colors(ods)

            # close data sources & write out
            ids = None
            ods = None

            # GeoTiff -> JPG
            output_file_resp = self.tif2jpg(output_file)

            Logger.debug(
                "File {} resampled from ({}, {}) to ({}, {}) resolution: {}".format(
                    input_file, itrans[1], itrans[5], otrans[1], otrans[5], output_file_resp
            ))

        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "Resampling failed: {}".format(e)
            )

        return output_file_resp

    def get_parent_identifier(self):
        """Get parent identifier from configuration.

        :return str: parent identifier
        """
        return self.config['catalog']['ip_parent_identifier'].replace(
            '{{}}', self.parent_identifier
        )


class QCProcessorMultiBase(QCProcessorBase):
    """Processor IP multi-sensor class.

    Supported platforms:
     - Sentinel-2
     - Landsat-8
    
    :param config: processor-related config file
    :param QCResponse response: processor QI metadata response managed by the manager
    """
    def __init__(self, config, response):
        super(QCProcessorMultiBase, self).__init__(
            config, response
        )

    @abstractmethod
    def processor_sentinel2(self):
        """Sentinel2-specific processor.
        """
        pass

    @abstractmethod
    def processor_landsat8(self):
        """Landsat8-specific processor.
        """
        pass

    def get_processor_sensor(self, pp, ptype):
        """Get processor sensor.

        :param str pp: sensor name (Sentinel-2 or Landsat-8)
        :param QCPlatformType ptype: platform type

        :return QCProcessorIPBase: processor
        """
        if pp == 'Sentinel-2':
            processor_class = self.processor_sentinel2()
        elif pp == 'Landsat-8':
            processor_class = self.processor_landsat8()
        else:
            raise ProcessorFailedError(
                self,
                "Unsupported platform '{}'".format(pp)
            )

        processor = processor_class(
            self.config, self._response
        )
        processor.set_identifier(self.identifier)
        processor.set_results()
        processor.set_platform_type(ptype)

        return processor

    def run(self):
        """Perform processor tasks.
        """
        self._run_start()

        # local data download and processing
        metapath = os.path.join(
            self.config['project']['path'], self.config['project']['metapath']
        )
        self.create_dir(metapath)

        # run platform-based search processor
        response_counter = -1
        for ptype in ['primary', 'supplementary']:
            try:
                pp = self.config['image_products']['{}_platform'.format(ptype)]
            except KeyError:
                pp = None
            if pp is None:
                Logger.debug("{} platform not defined, skipped".format(
                    ptype.capitalize()
                ))
                continue
            Logger.debug(
                'Platform ({}): {}'.format(ptype, pp)
            )
            processor = self.get_processor_sensor(pp, ptype)

            # run processor & update response counter
            processor.set_response_counter(response_counter)
            response_counter = processor.run()
            # collect results
            self._result['qi.files'] = set()
            for k, v in processor.result().items():
                if k.startswith('ip_operation'):
                    self._result[k] += v
                elif k == 'qi.files':
                    for vv in v.values():
                        self._result[k].add(vv)

        self._run_done()


class QCProcessorLPBase(QCProcessorBase):
    """Processor land product base class.

    :param config: processor-related config file
    :param QCResponse response: processor QI metadata response managed by the manager
    """
    def __init__(self, config, response):
        super(QCProcessorLPBase, self).__init__(config, response)

        try:
            self.check_dependency()
        except ImportError as e:
            raise ProccessorDependencyError(self, e)

    @abstractmethod
    def check_dependency(self):
        """Check dependicies.

        Raise ProcessorDependencyError of failure.
        """
        pass

    def lp_operation(self, status, timestamp=None):
        """Log LP operation.

        :param DbIpOperationStatus status: status
        :param datetime: timestamp or None
        """
        self._result['lp_operation.{}'.format(status.name)] += 1
        Logger.ip_operation(
            "{} processor: LP operation completed ({})".format(
                self.identifier, status.name),
            identifier=self.identifier,
            timestamp=timestamp,
            status=status.value
        )

    def reset(self):
        """Reset processor results."""
        for status in DbIpOperationStatus:
            self._result['lp_operation.{}'.format(status.name)] = 0

    def run(self):
        """Perform processor tasks.
        """
        # log start computation
        self._run_start()

        # run computation
        response_data = self._run()

        # update response
        if response_data:
            self.set_response_counter(0) # assuming one response only
            self.update_response(response_data)

        if self.get_response_status() is None:
            self.set_response_status(DbIpOperationStatus.forced)

        # log computation finished
        self._run_done()


def identifier_from_file(filepath):
    """Determine identifier from filepath.

    :param filepath: file path
    """
    identifier = os.path.splitext(
        os.path.basename(filepath))[0]
    if identifier == '__init__':
        identifier = os.path.splitext(
            os.path.basename(os.path.dirname(filepath)))[0]

    return identifier
