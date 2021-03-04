import os
import tempfile
import subprocess
import shutil

from manager.logger.db import DbIpOperationStatus
from manager.logger import Logger

from processors.exceptions import ProcessorFailedError
from processors.cloud_coverage.base import QCProcessorCloudCoverageBase
from processors.sentinel import QCProcessorSentinelMeta


class QCProcessorCloudCoverageSentinel(QCProcessorCloudCoverageBase, QCProcessorSentinelMeta):
    level2_data = False

    def __init__(self, config, response):
        super(QCProcessorCloudCoverageSentinel, self).__init__(
            config, response
        )

        self.comp_res = 20
        # results
        # -> self._result['qi.files']['fmask']
        self.add_qi_result(
            'fmask',
            '{}m.img'.format(self.comp_res)
        )

    def check_dependency(self):
        import fmask
        from osgeo import gdal

    @staticmethod
    def get_lh_dir(data_dir):
        """Get data_dir with L2A changed to LH."""
        return data_dir.replace('L2A', 'L2H')

    @staticmethod
    def get_lineage():
        import fmask

        return 'http://qcmms.esa.int/Python_Fmask_S2_v{}'.format(
            fmask.__version__
        )

    def compute(self, data_dir, output_dir):
        """Compute cloud coverage

        :param str data_dir: input data directory
        :param str output_dir: output directory

        :return str: output file
        """
        import fmask

        # compute mask in 20m resolution
        try:
            self.run_fmask(data_dir, self._result['qi.files']['fmask'], self.comp_res)
        except subprocess.CalledProcessError as e:
            Logger.error(
                "fmask failed: {}".format(e)
            )
            self.set_response_status(DbIpOperationStatus.failed)
            return None

        return self._result['qi.files']['fmask']

    def run_fmask(self, data_dir, output, resolution=20):
        # pixsize shall be the target spatial resolution from LPST OR
        # default --pixsize 20 and then resample to target res. =>
        # faster -e TEMPDIR for faster SSD processing

        Logger.info("Running fmask for {}".format(data_dir))
        tmppath = tempfile.mkdtemp()
        subprocess.run(['fmask_sentinel2Stacked.py',
                        '--pixsize', str(resolution),
                        '-o', output,
                        '-e', tmppath,
                        '--safedir', data_dir],
                       check=True
        )

        shutil.rmtree(tmppath)
        Logger.debug("fmask temp directory removed: {}".format(tmppath))

        if output:
            Logger.info("Output from fmask: {}".format(output))

        # Check fmask version(?) -> metadata lineage = python-fmask-0.5.3
        # Verify fmask spatial resolution -> metadata
        
    def compute_cc_stats(self, meta_data, data_dir, output_dir):
        from osgeo import gdal, gdalconst, ogr, osr
        gdal.UseExceptions()

        # clouds file
        try:
            filename = 'MSK_CLOUDS_B00.gml'
            # copy to output directory
            shutil.copyfile(
                self.filter_files(data_dir, filename)[0],
                os.path.join(output_dir, filename)
            )
            mask_file = os.path.join(output_dir, filename)
        except IndexError:
            raise ProcessorFailedError(
                self,
                "Mask file not found for {}".format(os.path.basename(meta_data['title']))
            )

        # clouds percentage
        cloudsPct = -1 # unknown
        try:
            clouds_area = footprint_area = 0

            # 1. compute clouds area
            ds = ogr.Open(mask_file)
            layer = ds.GetLayer()
            # assuming no clounds are presented if layer is None
            if layer:
                for feat in layer:
                    geom = feat.GetGeometryRef()
                    clouds_area += geom.GetArea()
            ds = None

            if clouds_area <= 0:
                # assuming no clounds are presented
                cloudsPct = 0.0
            else:
                # 2. compute footprint area
                wkt = meta_data['footprint']
                footprint = ogr.CreateGeometryFromWkt(wkt)

                # transformation is needed in order to compute area in sq meters
                src_srs = osr.SpatialReference()
                src_srs.ImportFromEPSG(4326) # footprint should be in WGS-84
                # use band srs as target
                try:
                    img_ref_file = self.filter_files(data_dir, 'B01.jp2')[0]
                except IndexError:
                    raise ProcessorFailedError(
                        self,
                        "No image reference found in {}".format(data_dir)
                    )
                Logger.debug("Using {} as reference crs".format(img_ref_file))
                ds = gdal.Open(img_ref_file)
                tgt_srs = osr.SpatialReference()
                tgt_srs.ImportFromWkt(ds.GetProjectionRef())
                transform = osr.CoordinateTransformation(src_srs, tgt_srs)

                footprint_utm = footprint.Clone()
                footprint_utm.Transform(transform)
                footprint_area = footprint_utm.GetArea()
                footprint_utm = footprint = None

                cloudsPct = (clouds_area / footprint_area) * 100
        except (RuntimeError, KeyError) as e:
            raise ProcessorFailedError(
                self,
                "Computing clouds percentage failed: {}".format(e)
            )

        response_data = {
            "id": "http://qcmms.esa.int/detailed_control#CLOUD_COVER",
            'cloudPct': int(cloudsPct),
            'mask': self.file_basename(mask_file),
            'lineage':'http://qcmms.esa.int/ESA_OLQC_SC_v{}'.format(
                meta_data["Processing baseline"]),
        }

        return response_data
