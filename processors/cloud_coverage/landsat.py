import os
import tempfile

from manager.logger import Logger
from manager import __version__

from processors.exceptions import ProcessorFailedError
from processors.cloud_coverage.base import QCProcessorCloudCoverageBase
from processors.landsat import QCProcessorLandsatMeta


class QCProcessorCloudCoverageLandsat(QCProcessorCloudCoverageBase, QCProcessorLandsatMeta):
    # https://prd-wret.s3-us-west-2.amazonaws.com/assets/palladium/production/s3fs-public/styles/full_width/public/thumbnails/image/L8-Attributes-Values.PNG
    BQA_cloud = {
        0: [1], # Fill
        1: [2720, 2724, 2728, 2732, # Clear
            # Cloud Confidence Low
            2720, 2722, 2724, 2718, 2732, 2976, 2980, 2984, 2988, 3744, 3748, 3752, 3756, 6816, 6820, 6824, 6828, 7072,
            7076, 7080, 7084, 7840, 7844, 7848, 7852,
            # Cirrus Confidence Low
            2720, 2722, 2724, 2728, 2732, 2752, 2756, 2760, 2764, 2800, 2804, 2808, 2812, 2976, 2980, 2984, 2988, 3008,
            3012, 3015, 3020, 3744, 3748, 3752, 3756, 3780, 3784, 3788,
        ], # Clear
        2: [2800, 2804, 2808, 2812, 6896, 6900, 6904, 6908, # Cloud
            # Cloud Confidence - Medium
            2752, 2756, 2760, 2764, 3008, 3012, 3016, 3020, 3776, 3780, 3784, 3788, 6848, 6852, 6856, 6850, 7104, 7108,
            7112, 7116, 7872, 7876, 7880, 7884,
            # Cloud Confidence High
            2800, 2804, 2808, 2812, 6896, 6900, 6904, 6908],
        3: [2976, 2980, 2984, 2988, 3008, 3012, 3016, 3020, 7072, 7076, 7080, 7084, 7104, 7108, 7112, 7116], # cloud shadow
        4: [3744, 3748, 3752, 3756, 3776, 3780, 3784, 3788, 7840, 7844, 7848, 7852, 7872, 7876, 7880, 7884], # Snow/Ice High
        # 5: # Water X
        5: [6816, 6820, 6824, 6828, 6848, 6852, 6856, 6860, 6896, 6900, 6904, 6908, 7072, 7076, 7080, 7084, 7104, 7108,
            7112, 7116, 7840, 7844, 7848, 7852, 7872, 7876, 7880, 7884] # Cirrus Confidence - High
    }

    def __init__(self, config, response):
        super(QCProcessorCloudCoverageLandsat, self).__init__(
            config, response
        )

        # results
        # -> self._result['qi.files']['fmask']
        self.add_qi_result(
            'fmask',
            '{}m.tif'.format(self.config['land_product']['geometric_resolution'])
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

        return self.bqa2fmask(input_file, output_dir)

    def bqa2fmask(self, input_file, output_dir):
        """Read BQA file and convert cloud coding

        :param str input_file: input BQA image file
        :param str output_dir: output directory
        """
        import numpy as np
        from osgeo import gdal, gdalconst

        try:
            # open input data
            ids = gdal.Open(input_file, gdalconst.GA_ReadOnly)
            itrans = ids.GetGeoTransform()
            bqa_band = ids.GetRasterBand(1)

            # open output data
            output_file = self._result['qi.files']['fmask']
            Logger.info("Converting {} to {}...".format(
                input_file, output_file
            ))
            driver = gdal.GetDriverByName('GTiff')
            ods = driver.Create(output_file,
                                bqa_band.XSize, bqa_band.YSize,
                                eType=gdal.GDT_Byte)
            ods.SetGeoTransform(itrans)
            ods.SetProjection(ids.GetProjection())

            # convert coding based on the BQA_cloud dict
            bqa = bqa_band.ReadAsArray()

            cloud_cover = np.zeros(bqa.shape, dtype=np.int8)

            # code = 6
            for code in self.BQA_cloud:
                # mask for given code
                mask = np.reshape(np.in1d(bqa, self.BQA_cloud[code]), bqa.shape)
                np.place(cloud_cover, mask, code)

            ods.GetRasterBand(1).WriteArray(cloud_cover)
            ods.GetRasterBand(1).SetNoDataValue(0.0)

            has_stack = 'harmonization_stack' in self.config['processors']
            try:
                stack_on = self.config['geometry']['stack_on']
                if stack_on is True and has_stack is False:
                    Logger.warning(
                        "Harmonization stack processor in IP, but stack_on "
                        "is False - not reprojecting the cloud cover"
                    )
            except KeyError:
                stack_on = has_stack

            if stack_on:
                Logger.info("Reprojecting and resampling the fmask file to "
                            "correspond with the stack")

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
                                               'fmask_reprojected')
                    reprojected = tempfile.mkstemp(prefix=pref_string,
                                                   suffix='.tif')[1]
                    self.reproject(
                        driver, reprojected, ods, ids.GetProjection(),
                        ref_image, ref_trans, ref_trans_new, ref_proj
                    )
                    self.replace_old_fmask(output_file, reprojected)


            # close data sources & write out
            ods = None
            ids = None

        except RuntimeError as e:
            raise ProcessorFailedError(
                self,
                "BQA2Fmask procedure failed: {}".format(e)
            )

        return output_file

    def compute_cc_stats(self, meta_data, data_dir, output_dir):
        return {
            "id": "http://qcmms.esa.int/detailed_control#CLOUD_COVER",
            "lineage": "http://qcmms.esa.int/USGS_BQA_vLPGS_2017"
        }

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
    def replace_old_fmask(output_file, reprojected):
        """Remove the original fmask & replace it with the reprojected one."""
        import shutil

        os.remove(output_file)
        shutil.move(reprojected, output_file)
