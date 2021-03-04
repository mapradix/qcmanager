import os
from datetime import datetime
from abc import ABC, abstractmethod

from manager.logger import Logger

from styles import StyleReader

from processors import QCProcessorIPBase
from processors.exceptions import ProccessorDependencyError, ProcessorFailedError

try:
    import numpy as np
    from osgeo import gdal, gdalconst, gdal_array
    gdal.UseExceptions()
except ImportError as e:
    raise ProccessorDependencyError(self, e)


class QCProcessorValidPixelsBase(QCProcessorIPBase, ABC):
    """Validity pixel control processor abstract base class.

    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "detailedControlMetric"
    isMeasurementOfSection = 'validPixels'

    def __init__(self, config, response):
        super(QCProcessorValidPixelsBase, self).__init__(
            config, response
        )

        # results
        # -> self._result['qi.files']['output']
        self.add_qi_result(
            'output',
            '{}m.tif'.format(self.config['land_product']['geometric_resolution'])
        )

    @staticmethod
    @abstractmethod
    def get_lh_dir(data_dir):
        """Get data directory with L2 changed to L2H.

        :param data_dir: Path to data directory

        :return str: directory name
        """
        pass

    def check_dependency(self):
        """Check processors software dependencies.
        """
        from osgeo import gdal

    def _run(self, meta_data, data_dir, output_dir):
        """Perform processor tasks.
        :param meta_data: IP metadata
        :param str data_dir: path to data directory
        :param str output_dir: path to output processor directory

        :return dict: QI metadata
        """
        response_data = {
            self.isMeasurementOfSection: [
                {
                    "id": "http://qcmms.esa.int/detailed_control#VALID_PIXELS",
                    "generatedAtTime": datetime.now(),
                }
            ]
        }

        # get the arosics input for fmask
        try:
            mask_raster = self._get_arosics_raster(output_dir)
        except ProcessorFailedError:
            return response_data

        # create fmask tif
        Logger.info("Running vpx for {} (mask: {})".format(data_dir, mask_raster))
        output = self._result['qi.files']['output']
        try:
            self._run_vpx(mask_raster, output)
        except ProcessorFailedError:
            return response_data

        # GeoTiff mask -> JPG mask
        output_jpg = self.tif2jpg(output)

        Logger.info("Output from valid pixels: {}".format(output_jpg))

        # additional response attributes
        response_data[self.isMeasurementOfSection][0].update(
            {
                'validPct': self._compute_stats(output),
                'mask': self.file_basename(output_jpg),
                'rasterCoding': self.config['pixel_metadata_coding'][self.identifier],
                'lineage': self.get_lineage()
            }
        )

        return response_data

    def _run_vpx(self, input_file, output, inverse_mask=False):
        """Run valid pixels computation.

        Raise ProcessorFailedError on failure.

        :param str input_file: input raster file
        :param str output: output raster file
        :param bool inverse_mask: use inverse mask?
        """
        try:
            # open input data
            ids = gdal.Open(input_file, gdalconst.GA_ReadOnly)
            iproj = ids.GetProjection()
            itrans = ids.GetGeoTransform()
            fmask_band = ids.GetRasterBand(1)
            # ct = band_ref.GetRasterColorTable()

            # open output data
            driver = gdal.GetDriverByName('GTiff')
            ods = driver.Create(output,
                                fmask_band.XSize, fmask_band.YSize,
                                eType=fmask_band.DataType)
            ods.SetGeoTransform(itrans)
            ods.SetProjection(iproj)

            # create vpx
            dtype = gdal_array.GDALTypeCodeToNumericTypeCode(
                ids.GetRasterBand(1).DataType
            )
            fmask_arr = np.array(fmask_band.ReadAsArray(), dtype=dtype)

            if inverse_mask:
                vpx_boolean = np.logical_and(fmask_arr != 1, fmask_arr != 5)
            else:
                vpx_boolean = np.logical_or(fmask_arr == 1, fmask_arr == 5)

            vpx_arr = vpx_boolean * 1
            
            # add radiometry saturation pixels
            try:
                rad_pattern = 'radiometry_control_{}m.tif$'.format(
                    self.config['land_product']['geometric_resolution']
                )
                radio_input_file = self.filter_files(
                    os.path.dirname(input_file), pattern=rad_pattern
                )[0]

                ids_ = gdal.Open(radio_input_file, gdalconst.GA_ReadOnly)
                radio_band = ids_.GetRasterBand(1)
                radio_arr = radio_band.ReadAsArray()
                if (radio_arr > 0).any():
                    np.place(vpx_arr, radio_arr > 0, [0])
            except IndexError:
                Logger.info("Radiometry file not found in {}.".format(
                    os.path.dirname(input_file)
                ))

            # save vpx
            ods.GetRasterBand(1).WriteArray(vpx_arr)
            # ods.GetRasterBand(1).SetNoDataValue(fmask_band.GetNoDataValue())

            # set color table
            style_r = StyleReader(os.path.basename(os.path.dirname(__file__)))
            style_r.set_band_colors(ods)

            # cls data sources & write out
            ids = None
            ods = None

        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "VPX processor failed: {}".format (e)
            )

    def _get_arosics_raster(self, data_dir):
        """Get raster produced by Arosics package.
        
        :param str data_dir: image product data directory

        :return str: file path
        """
        fmask_file = 'cloud_coverage_{}m.tif'.format(
            self.config['land_product']['geometric_resolution']
        )

        try:
            filepath = self.filter_files(data_dir, pattern=fmask_file+'$')[0]
            return filepath
        except IndexError:
            raise ProcessorFailedError(
                self,
                "No mask file found in {}".format(
                    data_dir
            ))

    def _compute_stats(self, filename):
        """Compute valid pixels statistics.

        :param str filename: input raster file

        :return int: valid pixels percentage
        """
        value, count, ncells = self.compute_value_count(filename)

        vp_Pct = 0.0
        for idx in range(len(value)):
            if value[idx] == 1:
                vp_Pct = count[idx] / ncells * 100

        return int(vp_Pct)
