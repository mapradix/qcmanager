"""Test suite checking processors' performance on TUC2."""

import os
import pytest

from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus

from test_tc import TestsBase, _cleanup, _setup, _teardown

config_files = [
    os.path.join(
        os.path.dirname(__file__), '..', 'config.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc2_tccm_1518_020m', 'tuc2_tccm_2015_2018_20m_sumava.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc2_tccm_1518_020m', 'tuc2_tccm_2015_2018_20m_sumava_sample.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'test_tc2.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'test.yaml')
]


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    _cleanup(request, config_files)


@pytest.fixture(scope='class')
def class_manager(request):
    _setup(request, config_files)
    yield
    _teardown(request)


@pytest.mark.usefixtures('cleanup')
@pytest.mark.usefixtures('class_manager')
class TestsTUC2(TestsBase):
    """Test class (currently) for TUC2.

    Contains tests for TUC2 - both implemented and simple calls of the base
    methods.
    """
    def test_tc_030(self):
        """Get LPST for multi-sensor time-series.

        This test case consists to check that the QC Manager retrieves and
        parse land product specification table specific to definitions for
        multi-sensor time-series production.
        """
        self.do_001_030()

        Logger.info("Getting LPST for multi-sensor time-series")

    def test_tc_031(self):
        """Get metadata of all multi-sensor acquired IP.

        This test case consists to check that the QC Manager is getting
        metadata of the defined multi-sensor IP.
        """
        self.do_002a_031()

        Logger.info("Getting metadata of all multi-sensor acquired IP")

    def test_tc_032(self):
        """Select multi-sensor time-series IPs that passed QC.

        This test case consists to check that the QC Manager selects
        multi-sensor time series IP that passed QC.
        """
        self.do_003_032()

        Logger.info("Feasibility processed")
        Logger.info("Selecting multi-sensor time-series IPs that passed QC")

    def test_tc_033a(self):
        """Check completeness of the multi-sensor IP.

        This test case consists to check that the QC manager checks
        completeness of the multi-sensor IP.
        """
        self.do_004a_033a()
        self.do_004b_033a()

        Logger.info("Checking completeness of the multi-sensor IP")

    def test_tc_033b(self):
        """Check consistency of the multi-sensor IP.

        This test case consists to check that the QC Manager checks
        consistency of the multi-sensor IP.
        """
        self.set_response_type('ip')

        from processors.l2_calibration import QCProcessorL2Calibration
        from processors.ordinary_control import QCProcessorOrdinaryControl
        from processors.harmonization_stack import QCProcessorHarmonizationStack
        from osgeo import gdal

        # run l2_calibration
        processor = QCProcessorL2Calibration(
            self._manager.config, self._manager.response
        )
        processor.run()

        assert processor.get_response_status() != DbIpOperationStatus.failed

        # run ordinary control
        processor = QCProcessorOrdinaryControl(
            self._manager.config, self._manager.response
        )
        processor.run()

        assert processor.get_response_status() != DbIpOperationStatus.failed

        # run harmonization control
        processor = QCProcessorHarmonizationStack(
            self._manager.config, self._manager.response
        )
        processor.run()

        # get attributes of the reference image
        ref_im_path = self._manager.config['geometry']['reference_image']

        ref_im = gdal.Open(ref_im_path, gdal.GA_ReadOnly)
        # get pixel area from georeference of raster
        geo = ref_im.GetGeoTransform()
        # get the only band
        band = ref_im.GetRasterBand(1)

        rows = band.YSize
        cols = band.XSize
        dtype = band.DataType
        bits = 16 if dtype == 2 else 8
        res = abs(geo[1])

        self.check_responses(
            'ordinaryControlMetric',
            ({'harmonized': 'tile'}, 0, {'raster': 'rows'}),
            self.check_value_one(rows)
        )
        self.check_responses(
            'ordinaryControlMetric',
            ({'harmonized': 'tile'}, 0, {'raster': 'cols'}),
            self.check_value_one(cols)
        )
        self.check_responses(
            'ordinaryControlMetric',
            ({'harmonized': 'tile'}, 0, {'raster': 'bits'}),
            self.check_value_one(bits)
        )
        self.check_responses(
            'ordinaryControlMetric',
            ({'harmonized': 'tile'}, 0, {'raster': 'resolution'}),
            self.check_value_one(res)
        )

        Logger.info("Checking consistency of the multi-sensor IP")

        ref_im = None

    def test_tc_034a(self):
        """Check availability of pixel-level multi-sensor metadata.

        This test case consists to check that the QC Manager checks
        availability of pixel level multi-sensor metadata.
        """
        from processors.cloud_coverage import QCProcessorCloudCoverage

        processor_cc = QCProcessorCloudCoverage(
            self._manager.config, self._manager.response
        )
        processor_cc.run()
        assert processor_cc.get_response_status() != DbIpOperationStatus.failed

        from processors.geometry_quality import QCProcessorGeometryQuality

        processor_cc = QCProcessorGeometryQuality(
            self._manager.config, self._manager.response
        )
        processor_cc.run()
        assert processor_cc.get_response_status() != DbIpOperationStatus.failed

        # run harmonization control
        from processors.harmonization_control import QCProcessorHarmonizationControl

        processor = QCProcessorHarmonizationControl(
            self._manager.config, self._manager.response
        )
        processor.run()

        self.do_009_034a()
        Logger.info("Checking availability of pixel-level "
                    "multi-sensor metadata")

    def test_tc_034b(self):
        """Check lineage of pixel-level multi-sensor metadata.

        This test case consists to check that the QC Manager checks lineage
        of the pixel-level multi-sensor metadata.
        """
        self.do_011_034b()
        Logger.info("Checking lineage of pixel-level multi-sensor metadata")

    def test_035(self):
        """Check consistency of multi-sensor time-series pixel-level IP metadata.

        This test case consists to check that the QC Manager checks
        consistency of multi-sensor time-series pixel-level IP metadata
        """
        def get_pixel_values(sec):
            values = []
            for item in self._manager.config['pixel_metadata_coding'][rc]:
                values.append(item['min'])
            return values

        for rc, sec in (('cloud_coverage', 'cloudCover'),
                        ('valid_pixels', 'validPixels')):
            self.check_responses('detailedControlMetric', {sec: 'mask'},
                                 self.check_pixel_coding(get_pixel_values(rc)))
        Logger.info("Checking consistency of multi-sensor time-series "
                    "pixel-level IP metadata")

    def test_036(self):
        """Create temporal coverage.

        This test case consists to check that the QC Manager creates temporal
        coverage layers based on identified set of input IP and defined time
        step criteria.
        """
        self.do_012_036()
        Logger.info("Creating temporal coverage")

    def test_tc_037a(self):
        """Compare temporal coverage with specification.

        This test case consists to check that the QC Manager compares the
        temporal coverage with defined requirements in LPST.
        """
        self.do_013a_037a()
        Logger.info("Comparing temporal coverage with specification")

    def test_tc_037b(self):
        """Returning temporal coverage statistics.

        This test case consists to check that the QC Manager creates temporal
        coverage comparison statistics.
        """
        self.do_013b_037b()
        Logger.info("Returning temporal coverage statistics")

    def test_tc_038a(self):
        """Identify cross-sensor IP metric.

        This test case consists to check that the QC Manager identifies pairs
        of image products to be passed to cross-sensor comparison.
        """
        self.set_response_type('ip', copy_responses=True)
        self.check_responses('harmonizationControlMetric', 'value',
                             self.check_value_type(bool))
        Logger.info("Identifying cross-sensor IP metric")

    def test_tc_038b(self):
        """Create cross-sensor comparison indicators.

        This test case consists to check that the QC Manager calculates
        cross-sensor comparison indicators on selected parameter.
        """
        self.check_responses('harmonizationControlMetric',
                             ('geometryConsistency', 0, 'value'),
                             self.check_value_type(bool))
        Logger.info("Creating cross-sensor comparison indicators")

    def test_tc_039(self):
        """Create cross-sensor indicators with specification.

        This test case consists to check that the QC Manager compares
        the cross-sensor QI with specification table.
        """
        res = self._manager.config['land_product']['geometric_resolution']
        self.check_responses(
            'harmonizationControlMetric',
            ('geometryConsistency', 0, 'medianAbsShift'),
            lambda value: value < res
        )
        Logger.info("Creating cross-sensor indicators with specification")

    def test_tc_041a(self):
        """ Check availability of the time-series LP metadata

        This test case consists to check that the QC Manager identifies
        Land Product associated metadata.
        """
        self.set_response_type('lp', copy_responses=True)

        from processors.lp_metadata_control import QCProcessorLPMetadataControl
        processor = QCProcessorLPMetadataControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('lpMetadataControlMetric', 'metadataAvailable',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Checking availability of the time-series LP metadata")

    def test_tc_041b(self):
        """Check consistency of the time-series LP metadata.

        This test case consists to check that the QC Manager checks
        consistency of the time-series LP metadata.
        """
        self.do_022b_041b()

        Logger.info('Checking consistency of the time-series LP metadata')

    def test_tc_042(self):
        """Create time-series LP validation indicators

        This test case consists to check that the QC Manager creates
        time-series LP validation indicators.
        """
        self.set_response_type('lp')
        from processors.lp_thematic_validation_control import QCProcessorLPThematicValidationControl
        processor = QCProcessorLPThematicValidationControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.do_024b_042()

        Logger.info('Creating time-series LP validation indicators')

    def test_tc_043(self):
        """Check values of time-series LP validation indicators

        This test case consists to check that the QC Manager
        checks values of time-series LP validation indicators.
        """

        self.do_025_043()

        Logger.info('Checking values of time-series LP validation indicators')
