from abc import ABC

import os
import glob
from datetime import datetime
from collections import OrderedDict
from osgeo import gdal, gdal_array, gdalconst
import numpy as np

from manager.logger import Logger # must be called before processors.exceptions
from manager.logger.db import DbIpOperationStatus

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorFailedError


class QCProcessorHarmonizationControlBase(QCProcessorIPBase, ABC):
    """Harmonization control processor abstract base class.
    
    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "harmonizationControlMetric"

    def __init__(self, config, response):
        super(QCProcessorHarmonizationControlBase, self).__init__(
            config, response
        )

        self.tc_std_threshold = 1000
        self.tsi_threshold = 0.5

        self._radiometry_consistency = None

    def check_dependency(self):
        """Check processor's software dependecies."""
        from osgeo import ogr
        import numpy as np
        import arosics

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
            "value": True
        }

        if self._radiometry_consistency is None:
            # run only once
            self._radiometry_consistency = self.get_radiometry_consistency(data_dir)

        response_data.update({
            "geometryConsistency": self.get_geometry_consistency(data_dir),
            "radiometryConsistency": self._radiometry_consistency
        })

        return response_data

    def get_geometry_consistency(self, data_dir):
        """Get QI response metadata corresponding to geometry consistency.

        :param data_dir: path to data directory

        :return: QI metadata
        """
        from osgeo import ogr
        import arosics
        import numpy as np

        correct_shifts = self.config['geometry'].get('correct_shifts', False)

        # find input GML file
        gml = 'geometry_quality_tie_points_{}_correction.gml'.format(
            'after' if correct_shifts else 'before'
        )
        try:
            input_file = self.filter_files(data_dir, gml)[0]
        except IndexError:
            raise ProcessorFailedError(
                self,
                "No file named {} found in {}".format(gml, data_dir)
            )

        # count stats from GML file
        gml_object = ogr.Open(input_file)
        layer = gml_object.GetLayer()

        abs_shift = []
        x_shift_m = []
        y_shift_m = []
        for i in layer:
            abs_shift.append(i.GetField('ABS_SHIFT'))
            x_shift_m.append(i.GetField('X_SHIFT_M'))
            y_shift_m.append(i.GetField('Y_SHIFT_M'))

        abs_shift_median = np.median(abs_shift)
        x_m_max = np.max(x_shift_m)
        y_m_max = np.max(y_shift_m)
        rmse_x = np.sqrt((np.array(x_shift_m) ** 2).mean())
        rmse_y = np.sqrt((np.array(y_shift_m) ** 2).mean())

        response_data = [
            {
                "id": '{}#COREGISTRATION'.format(self._measurement_prefix),
                "lineage": 'http://qcmms.esa.int/Arosics_v{}'.format(
                    arosics.__version__),
                "value": True,
                "rmseX": round(rmse_x, 1),
                "rmseY": round(rmse_y, 1),
                "diffXmax": round(x_m_max, 1),
                "diffYmax": round(y_m_max, 1),
                "medianAbsShift": round(abs_shift_median, 1)
            }
        ]

        # geometric accuracy level
        threshold = self.config['land_product']['geometric_accuracy'] * \
            self.config['land_product'].get('geometric_resolution')
        Logger.debug("Geometry quality threshold: {}m".format(threshold))
        npassed = 0
        for val in abs_shift:
            if val <= threshold:
                npassed += 1
        Logger.debug(
            "Validity check: number of points: {} (passed: {})".format(
                len(x_shift_m), npassed
            )
        )
        if npassed == 0:
            Logger.debug("No passed point -> Rejected")
            self.set_response_status(DbIpOperationStatus.rejected)
            response_data['value'] = False

        # add coregistrationErrorReductionPct
        if correct_shifts is True:
            gml_before = gml.replace('after', 'before')
            gml_before_file = self.filter_files(data_dir, gml_before)[0]
            gml_before_object = ogr.Open(gml_before_file)
            layer_before = gml_before_object.GetLayer()

            abs_shift_before = [i.GetField('ABS_SHIFT') for i in layer_before]
            median_before = np.median(abs_shift_before)
            coreg_pct = int(
                round((1 - abs_shift_median / median_before) * 100)
            )
            response_data[0].update(
                {"coregistrationErrorRedutionPct": coreg_pct}
            )
            gml_before_object = None

        gml_object = None

        return response_data

    def resample_img(self, in_file, out_file, resolution=100):
        """Resample image file to given resolution.

        :param str in_file: input raster name
        :param str out_file: output raster name
        :param int resolution : target resolution for resampling
        """
        input = gdal.Open(in_file, gdalconst.GA_ReadOnly)
        inputProj = input.GetProjection()
        inputTrans = input.GetGeoTransform()
        nbands = input.RasterCount
        bandreference = input.GetRasterBand(1)

        # adjust raster metadata
        x = int(np.round(input.RasterXSize / (resolution / inputTrans[1]), 0))
        y = int(np.round(input.RasterYSize / (resolution / inputTrans[1]), 0))
        targetTrans = (
        inputTrans[0], float(resolution), inputTrans[2], inputTrans[3], inputTrans[4], float(-resolution))

        driver = gdal.GetDriverByName('GTiff')
        output = driver.Create(out_file, x, y, nbands, bandreference.DataType)
        output.SetGeoTransform(targetTrans)
        output.SetProjection(inputProj)
        gdal.ReprojectImage(input, output, inputProj, inputProj, gdalconst.GRA_Bilinear)

        del output
        del input

    def resample_ips(self, data_dir, output_dir, resolution=100):
        """Resample all harmonized co-registered stack images.

        :param str data_dir: input data directory
        :param str output_dir: output data directory
        :param int resolution: target resolution

        :return list stack_ips: list of resampled images
        """
        in_prefix = ''
        if self.config['geometry'].get('correct_shifts', False):
            in_prefix += 'coreg_'
        if self.config['geometry'].get('stack_on', True):
            in_prefix += 'stack_'

        in_ips = self.filter_files(data_dir, pattern='{}.*.tif$'.format(
            in_prefix
        ))
        if len(in_ips) < 1:
            raise ProcessorFailedError(
                self,
                "No IP ({}) found in {}".format(in_prefix, data_dir)
            )

        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        stack_ips = []
        i = 0
        for ip in in_ips:
            ip_dirname = os.path.basename(os.path.dirname(ip))
            ip_dir = os.path.join(
                output_dir,
                ip_dirname
            )
            ip_img = os.path.join(
                ip_dir,
                ip.split(os.path.sep)[-1]
            )
            if not os.path.isdir(ip_dir):
                os.mkdir(ip_dir)


            # TODO: improve this fix 
            if not os.path.isfile(ip_img) or not self.filter_files(
                    ip_dir, pattern='cloud_coverage_.*.tif'):
                try:
                    self.resample_img(ip, ip_img, resolution)
                except:
                    raise ProcessorFailedError(
                        self,
                        'Cannot process IP: {}'.format(
                            os.path.basename(ip)
                    ))
                # resample cloud mask
                if ip_dirname.startswith('S2'):
                    cloud_dir = os.path.dirname(ip).replace('L2H', 'L2A') + '.SAFE'
                elif ip_dirname.startswith('LC'):
                    cloud_dir = os.path.dirname(ip).replace('L2H', 'L2')
                else:
                    Logger.warning("Unsupported IP {}".format(ip_dirname))
                    continue

                cloud_name = 'cloud_coverage_'
                try:
                    cloud_in = self.filter_files(cloud_dir, pattern='{}.*.tif$'.format(
                        cloud_name
                    ))[0]
                except IndexError:
                    raise ProcessorFailedError(
                        self,
                        'Cannot find IP cloud cover in {}'.format(
                            os.path.basename(cloud_dir)
                    ))

                cloud_out = ip_img.replace(in_prefix, cloud_name)
                try:
                    self.resample_img(cloud_in, cloud_out, resolution)
                    stack_ips.append(ip_dirname)
                except:
                    raise ProcessorFailedError(
                        self,
                        'Cannot process IP cloud cover: {}'.format(
                            os.path.basename(cloud_in)
                    ))
            else:
                Logger.debug('Already processed IP: {}'.format(
                    os.path.basename(ip_img))
                )
                stack_ips.append(ip_dirname)

        return stack_ips

    def get_dates_images(self, ips, output_dir):
        """Extract IP dates and the image and cloud mask path.

        :param list ips: IP names
        :param str output_dir: output directory

        :return ordered dict: ordered dates with path-names
        """
        dates = {}

        for ip in ips:
            if ip.startswith('S2'):
                date_str = ip.split('_')[-1].split('T')[0]
            elif ip.startswith('LC'):
                date_str = ip.split('_')[-4]
            else:
                Logger.warning(
                    "Unsupported IP {}. Unable to determine date".format(ip)
                )
                continue
            date = datetime.strptime(date_str, '%Y%m%d')
            ip_dir = os.path.join(output_dir, ip)
            try:
                in_prefix = ''
                if self.config['geometry'].get('correct_shifts', False):
                    in_prefix += 'coreg_'
                if self.config['geometry'].get('stack_on', True):
                    in_prefix += 'stack_'
                ip_stack = self.filter_files(
                    ip_dir, pattern='{}.*.tif'.format(in_prefix))[0]
                ip_cc = self.filter_files(
                    ip_dir, pattern='cloud_coverage_.*.tif')[0]
            except IndexError:
                raise ProcessorFailedError(
                    self,
                    "Incomplete IP {} (missing stack or cloud cover)".format(
                        ip
                ))
            dates.update({date: {'stack': ip_stack, 'cc': ip_cc}})

        return OrderedDict(sorted(dates.items()))

    def get_ordinal_dates(self, dates):
        """Convert datetime dates to ordinal dates.

        :param list dates: IP dates

        :return list: ordinal dates
        """
        ordinal_dates = []
        for d in dates:
            ordinal_dates.append(d.toordinal())

        return ordinal_dates

    def get_image_attribute(self, image_filename):
        """Get image metadata attributes

        :param image_filename: filename of the image

        :return tuple: nrow, ncol, nband, dtype of the image attributes
        """
        try:
            image_ds = gdal.Open(image_filename, gdal.GA_ReadOnly)
        except RuntimeError:
            raise ProcessorFailedError(
                self,
                'Could not read image {}'.format(image_filename)
            )

        nrow = image_ds.RasterYSize
        ncol = image_ds.RasterXSize
        nband = image_ds.RasterCount
        dtype = gdal_array.GDALTypeCodeToNumericTypeCode(image_ds.GetRasterBand(1).DataType)

        image_ds = None

        return (nrow, ncol, nband, dtype)

    def read_image_array(self, image_filename, bands=None, dtype=None):
        """Read image into NumPy array.

        :param str image_filename: filename of the image
        :param list bands: list of bands to read into array
        :param dtype: data type of the image data

        :return ndarray: N-dimensional NumPy array of the image
        """
        try:
            ds = gdal.Open(image_filename, gdal.GA_ReadOnly)
        except RuntimeError:
            raise ProcessorFailedError(
                self,
                'Could not read image {}'.format(image_filename)
            )
        if bands:
            if not all([b in range(1, ds.RasterCount + 1) for b in bands]):
                raise ProcessorFailedError(
                    self,
                    'Image {i} ({n} bands) does not contain bands '
                    'specified (requested {b})'.format(
                        i=image_filename, n=ds.RasterCount, b=bands
                ))
        else:
            bands = range(1, ds.RasterCount + 1)

        if not dtype:
            dtype = gdal_array.GDALTypeCodeToNumericTypeCode(
                ds.GetRasterBand(1).DataType
            )

        nrow, ncol, nband, dtype = self.get_image_attribute(image_filename)

        img_arr = np.zeros((len(bands), nrow, ncol), dtype=np.float)
        img_arr[img_arr == 0] = np.nan

        b = 0
        for band in bands:
            data_band = ds.GetRasterBand(band)
            img_arr[b, :, :] = np.array(data_band.ReadAsArray(), dtype=np.float64)
            b += 1

        ds = None

        return img_arr

    def valid_data_mask(self, array, low=0, high=10000):
        """Create mask of valid values from image array.

        :param ndarray array: NumPy array of the image IP
        :param int low: minumum allowed value
        :param int high: maximum allowed value

        :return ndarray: NumPy mask True is valid pixel
        """
        mask_high = array < high
        mask_low = array > low

        return mask_high & mask_low

    def read_fmask_array(self, cmask_fn):
        """Read cloud cover fmask to NumPy array.

        :param str cmask_fn: file name of the mask

        :return ndarray: NumPy array of the mask
        """
        img_i_fmask = self.read_image_array(cmask_fn[0])
        img_i_fmask = img_i_fmask[0, :, :]

        return img_i_fmask


    def prepare_y(self, img, fmask, band, low, high):
        """Create masked image for TSI.

        :param ndarray img: image product NumPy array
        :param ndarray band: cloud cover NumPy array
        :param list band: bands of the image
        :param integer low: minimum acceptable digital number
        :param integer high: maximum acceptable digital number

        :return tuple: NumPy ndarrays of the mask and ndarray of the masked image
        """
        valid = self.valid_data_mask(img[band, :, :], low, high)
        mask = ((fmask == 1) & valid) * 1
        img_mask = (mask).astype(np.float)
        img_mask[img_mask == 0] = np.nan
        y = img[band, :, :] * img_mask

        return mask[0, :, :], y[0, :, :]

    def calculate_tsi(self, stack_ips, bands, output_dir):
        """Calculate the TSI index based on Vermote et al. 2009

        :param list stack_ips: stacked IP for TSI calcualtion
        :param list bands: bands to process
        :param str output_dir: output directory

        :return list: TSI index per band
        """
        dates_img = self.get_dates_images(stack_ips, output_dir)
        dates_srt = list(dates_img.keys())
        ordinal_dates = self.get_ordinal_dates(dates_srt)

        # sort mix of Sentinel-2 and Landsat-8 based on acquisition date
        ips_srt = []
        for d in dates_img.keys():
            ips_srt.append(dates_img[d])

        # TSI variables setup
        time_range_thr = 20
        low = 0
        high = 10000

        # time series interpolated points
        img_0 = self.read_image_array(dates_img[dates_srt[0]]['stack'])
        y_dif_cum = np.zeros(img_0.shape)

        n = 2
        for i in range(len(dates_srt) - 2):
            # triplets for interpolation
            d = (ordinal_dates[i], ordinal_dates[i + 1], ordinal_dates[i + 2])
            time_range = d[2] - d[0]
            Logger.debug('Time range of the triplet is: {}'.format(time_range))
            if time_range <= time_range_thr:
                n += 1
                img_i = self.read_image_array(dates_img[dates_srt[i]]['stack'])
                img_i_cc = self.read_image_array(dates_img[dates_srt[i]]['cc'])
                img_i1 = self.read_image_array(dates_img[dates_srt[i + 1]]['stack'])
                img_i1_cc = self.read_image_array(dates_img[dates_srt[i + 1]]['cc'])
                img_i2 = self.read_image_array(dates_img[dates_srt[i + 2]]['stack'])
                img_i2_cc = self.read_image_array(dates_img[dates_srt[i + 2]]['cc'])

                for band in bands:
                    y_valid3 = np.zeros(img_0.shape[1:3])
                    mask_i, yi = self.prepare_y(img_i, img_i_cc, band, low, high)
                    mask_i1, yi1 = self.prepare_y(img_i1, img_i1_cc, band, low, high)
                    mask_i2, yi2 = self.prepare_y(img_i2, img_i2_cc, band, low, high)

                    # identify pixels with 3 valid points in sequence & interpolate
                    y_valid3 += (((mask_i + mask_i1 + mask_i2) >= 3) * 1)
                    if (y_valid3 > 0).any():
                        yi_interpol = np.abs((np.abs((yi2 - yi) / (d[2] - d[0])) * (d[1] - d[0])) - yi)
                        yi_diff = yi1 - yi_interpol
                        yi_dif_norm = (yi_diff / yi1) ** 2
                        yi_dif_norm_0 = np.nan_to_num(yi_dif_norm)
                        y_dif_cum[band, :, :] += yi_dif_norm_0

        # Assess tsi per band
        tsi = np.zeros(img_0.shape)
        divisor = np.sqrt((n - 2))
        tsi_result = {}
        for i in range(tsi.shape[0]):
            if n > 2:
                tsi[i, :, :] = (1 / divisor) * np.sqrt(y_dif_cum[i])
                value = np.mean(tsi[i, :, :])
            else:
                value = -1
            tsi_result['B{:0>2}'.format(i + 1)] = value

        return tsi_result

    def calculate_tc_std(self, stack_ips, bands, output_dir):
        """Calculate the TC STD indexes based on Qui et al. 2019

        :param list stack_ips: stacked IP for TSI calcualtion
        :param list bands: bands to process
        :param str output_dir: output directory

        :return list: TC STD index per band
        """
        dates_img = self.get_dates_images(stack_ips, output_dir)
        dates_srt = list(dates_img.keys())
        ordinal_dates = self.get_ordinal_dates(dates_srt)

        # sort mix of Sentinel-2 and Landsat-8 based on acquisition date
        ips_srt = []
        for d in dates_img.keys():
            ips_srt.append(dates_img[d])

        # Qui et al. 2019 params
        low = 0
        high = 10000
        img_0 = self.read_image_array(dates_img[dates_srt[0]]['stack'])
        img_arr = np.zeros((len(dates_srt), img_0.shape[0], img_0.shape[1], img_0.shape[2]))

        for i in range(len(dates_srt)):
            img = self.read_image_array(ips_srt[i]['stack'])
            img_cc = self.read_image_array(ips_srt[i]['cc'])
            Logger.debug("TC_STD: {}".format(os.path.basename(ips_srt[i]['stack'])))

            for band in bands:
                valid = self.valid_data_mask(img[band, :, :], low, high)
                mask = ((img_cc == 1) & valid) * 1
                img_mask = mask.astype(np.float)
                img_mask[img_mask == 0] = np.nan
                img_arr[i, band, :, :] = img[band, :, :] * img_mask

        tc_std = np.zeros(img_0.shape)
        std_result = {}
        for i in range(img_arr.shape[1]):
            tc_std[i, :, :] = np.nanstd(img_arr[:, i, :, :], axis=0)
            std_result['B{:0>2}'.format(i + 1)] = np.nanmean(tc_std[i, :, :])

        img_arr = None

        return std_result

    def get_radiometry_consistency(self, data_dir):
        """Get QI response metadata corresponding to radiometry consistency.

        :param str data_dir: path to data directory

        :return dict: QI metadata
        """
        # resample images to lower resolution
        resolution = 100
        in_dir = self.get_data_dir()
        output_dir = in_dir.replace(
            self.config['project']['downpath'],
            '{}_{}'.format(self.config['project']['downpath'], resolution)
        )
        Logger.debug('Output dir: {}'.format(output_dir))
        try:
            stack_ips = self.resample_ips(in_dir, output_dir, resolution)
        except IndexError:
            raise ProcessorFailedError(
                self,
                "Cannot resample image products."
            )

        # calculate TSI
        bands = list(range(6))
        tsi_val = self.calculate_tsi(stack_ips, bands, output_dir)
        Logger.debug("TSI: {}".format(tsi_val))

        # calculate Temporal Std
        tc_std_val = self.calculate_tc_std(stack_ips, bands, output_dir)
        Logger.debug("TC_STD: {}".format(tc_std_val))

        response_data = [
            {
                "id": "{}#TEMPORAL_CONSISTENCY".format(self._measurement_prefix),
                "lineage": self.get_lineage(),
                "value": True,
                "references": [
                    {
                        "category": "tc16Sd",
                        "title": "Qiu et al. (2019): Temporal Consistency"
                    },
                    {
                        "category": "tsiCE90",
                        "title": "Vermote et al. (2009): Temporal Smoothness Index"
                    }
                ],
                "bandsTemporalConsistency": []
            }
        ]

        for b in bands:
            bs = 'B{:0>2}'.format(b+1)
            response_data[0]["bandsTemporalConsistency"].append(
                {
                    "id": bs,
                    "tc16Sd": round(tc_std_val[bs], 2),
                    "tsiCE90": round(tsi_val[bs], 3)
                }
            )
            if tc_std_val[bs] > self.tc_std_threshold or \
               tsi_val[bs] > self.tsi_threshold:
                Logger.info("Band {}: rejected".format(bs))
                self.set_response_status(DbIpOperationStatus.rejected)
                response_data[0]['value'] = False

        return response_data
