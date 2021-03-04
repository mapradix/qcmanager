import os
import math
import datetime

from processors import QCProcessorLPBase, identifier_from_file, QCPlatformType
from processors.exceptions import ProcessorFailedError

from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus
from manager.io import JsonIO

from styles import StyleReader

class QCProcessorVpxCoverage(QCProcessorLPBase):
    """Valid pixels coverage control processor [coverage control].

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "ipForLpInformationMetric"
    level2_data = True

    def __init__(self, config, response):
        super(QCProcessorVpxCoverage, self).__init__(
            config, response
        )

        self.platform_type = None
        self.data_dir_suf = ''

    def check_dependency(self):
        """Check processor's software dependencies.
        """
        import numpy as np
        from osgeo import gdal, gdalconst, gdal_array

    def get_output_file(self, year):
        """Get output filename.

        :param int year: year
        
        :return str: target filename
        """
        return os.path.join(
            self.config['project']['path'],
            self.config['project']['downpath'],
            self.identifier + '_10m_' + str(year) + '.tif'
        )

    def get_years(self):
        """Get years from configuration.

        :return range: start-end year
        """
        return range(self.config['image_products']['datefrom'].year,
                     self.config['image_products']['dateto'].year+1)

    def compute_coverage(self):
        """Compute vpx coverage from input valid pixel masks.

        :return: path to output file
        """
        # collect years
        years = {}
        for yr in self.get_years():
            years[yr] = []

        # collect input files from last IP processor
        processed_ips = Logger.db_handler().processed_ips_last(
            'valid_pixels')

        ip_idx = 1
        ip_count = len(processed_ips)
        if ip_count == 0:
            # create empty vpx_coverage file
            from osgeo import gdal, gdalconst

            im_reference = self.config.abs_path(
                self.config['geometry']['reference_image']
            )
            ids = gdal.Open(im_reference, gdalconst.GA_ReadOnly)
            iproj = ids.GetProjection()
            itrans = ids.GetGeoTransform()
            vpx_band = ids.GetRasterBand(1)

            for yr in years.keys():
                out_file = self.get_output_file(yr)
                driver = gdal.GetDriverByName('GTiff')
                ods = driver.Create(out_file,
                                    vpx_band.XSize, vpx_band.YSize,
                                    eType=vpx_band.DataType)
                ods.SetGeoTransform(itrans)
                ods.SetProjection(iproj)

                ods = None

                self.tif2jpg(out_file)

            ids = None

            raise ProcessorFailedError(
                self,
                "No input valid layers found"
            )

        for ip, platform_type, status in processed_ips:
            Logger.info("Processing {}... ({}/{})".format(
                ip, ip_idx, ip_count
            ))
            ip_idx += 1

            # set current platform type
            self.platform_type = QCPlatformType(platform_type)
            if self.config['image_products'].get('{}_processing_level2'.format(
                    self.get_platform_type())) == 'S2MSI2A':
                self.data_dir_suf = '.SAFE'
            else:
                self.data_dir_suf = ''

            # delete previous results if needed
            if status not in (DbIpOperationStatus.unchanged, DbIpOperationStatus.rejected):
                do_run = True

            if self.get_last_ip_status(ip, status) == DbIpOperationStatus.rejected:
                Logger.info("{} skipped - rejected".format(ip))
                continue

            yr = self.get_ip_year(ip)
            data_dir = self.get_data_dir(ip)

            try:
                years[yr] += self.filter_files(
                    data_dir,
                    'valid_pixels_{}m.tif'.format(
                        self.config['land_product']['geometric_resolution']
                    )
                )
            except KeyError:
                raise ProcessorFailedError(
                    self,
                    'Inconsistency between years in metadata and years in the '
                    'config file. Years from the config file are {}, but you '
                    'are querying year {}'.format(years, yr)
                )

        vpx_files = {}
        for yr, input_files in years.items():
            if len(input_files) < 1:
                Logger.warning(
                    "No Vpx layers to be processed for {}".format(yr)
                )
                continue

            # define output file
            output_file = self.get_output_file(yr)
            vpx_files[yr] = output_file

            if os.path.exists(output_file):
                # run processor if output file does not exist
                continue

            status = DbIpOperationStatus.updated if os.path.exists(output_file) \
                else DbIpOperationStatus.added

            Logger.info("Running countVpx for {}: {} layers".format(
                yr, len(input_files)
            ))
            # run processor
            try:
                self.count_vpx(input_files, output_file)
            except ProcessorFailedError:
                pass

            # log processor IP operation
            if os.path.exists(output_file):
                timestamp = self.file_timestamp(output_file)
            else:
                timestamp = None
            # TBD
            ### self.lp_operation(status, timestamp=timestamp)

        return vpx_files

    def get_ip_year(self, ip):
        """Get image product filename.

        :param str ip: image product title

        :return dict: metadata
        """
        meta_data = JsonIO.read(
            os.path.join(
                self.config['project']['path'],
                self.config['project']['metapath'],
                ip + ".geojson"
            ))

        return meta_data['Sensing start'].year

    def count_vpx(self, input_files, output_file):
        """
        Perform valid pixels coverage.

        0: "noData", 1: "valid", 2: "clouds", 3: "shadows", 4: "snow", 5: "water"

        Raise ProcessorFailedError on failure.

        :param list input_files: list of input files
        :param str output_file: output filename
        """
        import numpy as np
        from osgeo import gdal, gdalconst, gdal_array

        try:
            # open input data
            ids = gdal.Open(input_files[0], gdalconst.GA_ReadOnly)
            iproj = ids.GetProjection()
            itrans = ids.GetGeoTransform()
            vpx_band = ids.GetRasterBand(1)

            # open output data
            driver = gdal.GetDriverByName ('GTiff')
            ods = driver.Create(output_file,
                                vpx_band.XSize, vpx_band.YSize,
                                eType=vpx_band.DataType)
            ods.SetGeoTransform(itrans)
            ods.SetProjection(iproj)

            # create countVpx array
            dtype = gdal_array.GDALTypeCodeToNumericTypeCode(
                ids.GetRasterBand(1).DataType
            )
            # dtype vs. no of images < 255 ?
            vpx_count = np.zeros((vpx_band.YSize, vpx_band.XSize), dtype=dtype)
            ids = None

            # count valid pixels
            for i in range(len(input_files)):
                ids = gdal.Open(input_files[i], gdalconst.GA_ReadOnly)
                vpx_band = ids.GetRasterBand(1)
                vpx_arr = vpx_band.ReadAsArray()

                vpx_count += vpx_arr

            # save count
            ods.GetRasterBand(1).WriteArray(vpx_count)
            # ods.GetRasterBand(1).SetNoDataValue(vpx_band.GetNoDataValue())

            # set color table
            StyleReader(self.identifier).set_band_colors(ods)

            # close data sources & write out
            ids = None
            ods = None

        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "Count Vpx processor failed: {}".format(e)
            )

    def compute_vpx_stats(self, vpx_file):
        """Compute stats for valid pixels coverage.

        :param str vpx_file: vpx file path

        :return dict: QI metadata
        """
        from osgeo import gdal

        # compute min/max
        value, count, ncells = self.compute_value_count(vpx_file)
        vpx_pct = 0.0
        for idx in range(len(value)):
            if value[idx] == 0:
                vpx_pct = count[idx] / ncells * 100

        data = {
            "min": int(min(value)),
            "max": int(max(value)),
            "gapPct": round(vpx_pct, 4),
            "mask": self.file_basename(self.tif2jpg(vpx_file))
        }
        ds = None

        return data

    def get_quality_indicators(self):
        """Get quality indicators.

        :return dict: QI metadata
        """
        vpx_timestamp = datetime.datetime.now()
        years = {}
        vpx_timestamp = None
        value = False
        fitness = 'FULL'
        for yr, vpx_file in self.compute_coverage().items():
            # take the last file
            if not vpx_timestamp:
                vpx_timestamp = self.file_timestamp(vpx_file)
            years[yr] = self.compute_vpx_stats(vpx_file)
            if value is False and years[yr]['max'] > 0:
                # at least one non-zero pixel
                value = True
            if fitness == 'FULL' and years[yr]['min'] == 0:
                # FULL -> PARTIAL: at least one zero pixel
                fitness = 'PARTIAL'
        if value is False:
            # no coverage
            fitness = 'NO'

        return {
            "value": value,
            "generatedAtTime": vpx_timestamp,
            "vpxCoverage": years,
            "fitnessForPurpose": fitness
        }

    def _run(self):
        """Run computation.

        :return dict: QI metadata
        """
        response_data = {
            'isMeasurementOf': '{}/#{}'.format(
                self._measurement_prefix, self.isMeasurementOf),
            'value' : False
        }

        response_data.update(self.get_quality_indicators())

        return response_data
