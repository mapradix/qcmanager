import os
import tempfile

from processors.radiometry_control.base import QCProcessorRadiometryControlBase
from processors.landsat import QCProcessorLandsatMeta
from processors.exceptions import ProcessorFailedError

from manager.logger import Logger


class QCProcessorRadiometryControlLandsat(QCProcessorRadiometryControlBase, QCProcessorLandsatMeta):

    # [10, 12, 34, 50]
    BQA_radiometry = {
        10: [2, 2722],  # Terrain Occlusion #
        # Radiometric Saturation - 1-2 bands
        12: [2724, 2756, 2804, 2980, 3012, 3748, 3780, 6820, 6852, 6900, 7076, 7108, 7844, 7876],
        # Radiometric Saturation - 3-4 bands
        34: [2728, 2760, 2808, 2984, 3016, 3752, 3784, 6824, 6856, 6904, 7080, 7112, 7848, 7880],
        # Radiometric Saturation - 5+ bands
        50: [2732, 2764, 2812, 2988, 3020, 3756, 3788, 6828, 6860, 6908, 7084, 7116, 7852, 7884]
    }

    def __init__(self, config, response):
        super(QCProcessorRadiometryControlLandsat, self).__init__(
            config, response
        )

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2 changed to LH."""
        return data_dir.replace('LC08_L2', 'LC08_L2H')

    def check_dependency(self):
        import numpy
        from osgeo import gdal

    def compute(self, data_dir, output_dir):
        """Compute cloud coverage

        :param str data_dir: input data directory
        :param str output_dir: output directory

        :return str: output file
        """
        try:
            input_file = self.filter_files(data_dir, 'BQA.TIF')[0]
        except IndexError:
            raise ProcessorFailedError(
                self,
                "No BQA image found in {}".format(data_dir)
            )

        return self.bqa2rq(input_file, output_dir)

    def bqa2rq(self, input_file, output_dir):
        """Read BQA file and convert radiometry saturation values

        :param str input_file: input BQA image file
        :param str output_dir: output directory

        :return str: output filename
        """
        import numpy as np
        from osgeo import gdal, gdalconst

        try:
            # open input data
            ids = gdal.Open(input_file, gdalconst.GA_ReadOnly)
            itrans = ids.GetGeoTransform()
            res = abs(itrans[1])
            bqa_band = ids.GetRasterBand(1)

            # open output data
            driver = gdal.GetDriverByName('GTiff')
            output_file = os.path.join(output_dir,
                                       '{}_{}m.tif'.format(self.identifier, int(res))
            )

            ods = driver.Create(output_file,
                                bqa_band.XSize, bqa_band.YSize,
                                eType=gdal.GDT_Byte)
            ods.SetGeoTransform(itrans)
            ods.SetProjection(ids.GetProjection())

            # convert coding based on the BQA_cloud dict
            bqa = bqa_band.ReadAsArray()

            radiometry_saturation = np.zeros(bqa.shape, dtype=np.int8)

            # [10, 12, 34, 50]
            # code = 6
            for code in self.BQA_radiometry:
                # mask for given code
                mask = np.reshape(np.in1d(bqa, self.BQA_radiometry[code]), bqa.shape)
                np.place(radiometry_saturation, mask, 1)

            # TBD add metadata
            rqsat_count = np.sum(radiometry_saturation > 0)
            rqsat_pct = (np.sum(radiometry_saturation > 0) /
                         (radiometry_saturation.shape[0] * radiometry_saturation.shape[0])) * 100
            ods.GetRasterBand(1).WriteArray(radiometry_saturation)
            ods.GetRasterBand(1).SetNoDataValue(0.0)

            Logger.info("Saturation: {} converted to {}...".format(
                input_file, output_file
            ))

            has_stack = 'harmonization_stack' in self.config['processors']
            try:
                stack_on = self.config['geometry']['stack_on']
                if stack_on is True and has_stack is False:
                    Logger.warning(
                        "Harmonization stack processor in IP, but stack_on "
                        "is False - not reprojecting the radiometry"
                    )
            except KeyError:
                stack_on = has_stack

            if stack_on:
                Logger.info("Reprojecting and resampling the radiometry file "
                            "to correspond with the stack")

                res = self.config['land_product']['geometric_resolution']
                ref_path = self.config['geometry'].get('reference_image')
                ref_image = gdal.Open(ref_path, gdalconst.GA_ReadOnly)
                ref_proj = ref_image.GetProjection()
                ref_trans = ref_image.GetGeoTransform()

                # update geotransform to new values
                ref_trans_list = list(ref_trans)
                ref_trans_list[1] = res
                ref_trans_list[5] = -res
                ref_trans_new = tuple(ref_trans_list)

                if ref_proj != ids.GetProjection() or ref_trans_new != itrans:
                    pref_string = os.path.join(tempfile.gettempdir(),
                                               'radiometry_reprojected')
                    reprojected = tempfile.mkstemp(prefix=pref_string,
                                                   suffix='.tif')[1]
                    self.reproject(
                        driver, reprojected, ods, ids.GetProjection(),
                        ref_image, ref_trans, ref_trans_new, ref_proj
                    )
                    self.replace_old_radiometry(output_file, reprojected, res)

            # close data sources & write out
            ods = None
            ids = None
        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "BQA2RQ procedure failed: {}".format(e)
            )

        return output_file

    @staticmethod
    def reproject(driver, out_file, in_image, in_proj, ref_image,
                  ref_trans, ref_trans_new, ref_proj):
        """Reproject and resample a band intended resolution.

        :param driver: Driver determining the format of the written file
        :param out_file: Path to the temporary file where the resampled band
            will be created
        :param in_image: Path to the original, not-resampled band
        :param in_proj: Projection of in_image
        :param ref_image: A GDAL object representing the reference image
        :param ref_trans: Geo transform (transformation coefficients) of
            the reference image
        :param ref_trans_new: Geo transform of the reference image, where
            the resolution is changed to the intended one
        :param ref_proj: Projection of ref_image
        """
        from osgeo import gdal, gdalconst
        rand_file = tempfile.mkstemp(prefix='resampled_5m_', suffix='.tif')[1]

        x = ref_image.RasterXSize
        y = ref_image.RasterYSize

        data_type = gdalconst.GDT_Byte

        ref_trans_list = list(ref_trans_new)
        ref_trans_list[1] = 5
        ref_trans_list[5] = -5
        ref_trans_5m = tuple(ref_trans_list)

        x_5m = int(x * ref_trans[1] / 5)
        y_5m = int(y * ref_trans[5] / -5)

        output_5m = driver.Create(rand_file, x_5m, y_5m, 1, data_type)
        output_5m.SetGeoTransform(ref_trans_5m)
        output_5m.SetProjection(ref_proj)

        gdal.ReprojectImage(in_image, output_5m, in_proj, ref_proj,
                            gdalconst.GRA_NearestNeighbour)

        x_target = int(x * ref_trans[1] / ref_trans_new[1])
        y_target = int(y * ref_trans[5] / ref_trans_new[5])

        output = driver.Create(out_file, x_target, y_target, 1, data_type)
        output.SetGeoTransform(ref_trans_new)
        output.SetProjection(ref_proj)

        gdal.ReprojectImage(output_5m, output, in_proj, ref_proj,
                            gdalconst.GRA_NearestNeighbour)

        output_5m = None
        output = None

        # delete temporary files with single resampled bands into a 5 m res
        os.remove(rand_file)

    @staticmethod
    def replace_old_radiometry(original_file, reprojected, res):
        """Remove the original radiometry file, move the reprojected one."""
        import shutil

        os.remove(original_file)
        original_file.replace('30m', '{}m'.format(res))
        shutil.move(reprojected, original_file)
