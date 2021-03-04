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
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'use_cases',
        'tuc1_imd_2018_010m', 'tuc1_imd_2018_010m_prague_sample.yaml'),
    os.path.join(
        os.path.dirname(__file__), '..', 'tests', 'test_tc1.yaml'),
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
class TestsTUC1(TestsBase):
    """Test class (currently) for TUC1.

    Contains tests for TUC1 - both implemented and simple calls of the base
    methods.
    """
    def check_responses(self, isMeasurementOf, attribute, condition,
                        check_missing_value=True):
        super(TestsTUC1, self).check_responses(isMeasurementOf, attribute, condition,
                                               check_missing_value)
        # check consistency
        response_type = self._response['current']
        assert self._num_responses[response_type] > 0
        assert len(self._response[response_type]) == self._num_responses[response_type]

    def test_tc_001(self):
        """Read LPST parameters

        This test case consists to check that the QC Manager reads the LPST
        correctly.
        """
        self.do_001_030()

        Logger.info("Reading LPST parameters")

    def test_tc_002a(self):
        """Create metadata request

        This test case consists to check that the QC Manager creates the
        metadata request and send it to the third party providers interface
        (e.g. sentinelsat).
        """
        self.do_002a_031()

        Logger.info("Creating metadata request")

    def test_tc_002b(self):
        """Get metadata based on request

        This test case consists to check that the QC Manager receives the IP
        metadata inline with the request parameters.
        """
        self.set_response_type('ip')

        from processors.search import QCProcessorSearch

        for ptype in ('primary', 'supplementary'):
            platform = self.get_platform(ptype)
            if not platform:
                continue
            processor = QCProcessorSearch(self._manager.config, self._manager.response).\
                get_processor_sensor(
                    self._manager.config['image_products']['{}_platform'.format(ptype)],
                    ptype
                )
            processor.connector = processor.connect()
            products = processor.query(
                processor.get_query_params()
            )
            self._num_responses['ip'] += len(products)

            for uuid in products:
                Logger.info("Querying {}".format(uuid))
                assert products[uuid]['producttype'] == \
                    self._manager.config['image_products']['{}_processing_level1'.format(
                        ptype)
                    ]
                date = products[uuid]['beginposition'].date()
                assert date > self._manager.config['image_products']['datefrom'] and \
                    date < self._manager.config['image_products']['dateto']

        Logger.info("Getting metadata based on request")

    def test_tc_003(self):
        """Select quality IP

        This test case consists to check that the QC Manager selects quality IP
        based on criteria filter applied on the metadata.
        """
        self.do_003_032()

        Logger.info("Feasibility processed")
        Logger.info("Selecting quality IP")

    def test_tc_004a(self):
        """Identify delivered IP

        This test case consists to check that the QC Manager identifies all
        delivered (locally downloaded) image products for further processing.
        """
        self.do_004a_033a()
        Logger.info("Identifying delivered IP")

    def test_tc_004b(self):
        """Compare delivery IP with expected

        This test case consists to check that the downloaded IP validated the MD5 checksum.
        """
        self.do_004b_033a()

        Logger.info("Comparing delivery IP with expected")

    def test_tc_005(self):
        """Identify IP raster layers

        This test case consists to check that the QC Manager identifies the image product raster
        layers representing the image bands.
        """
        self.set_response_type('ip')

        from processors.ordinary_control import QCProcessorOrdinaryControl

        processor = QCProcessorOrdinaryControl(
            self._manager.config, self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('ordinaryControlMetric', {'level1': 'rastersComplete'},
                             self.check_value_type(bool))
        Logger.info("Identifying IP raster layers")

    def test_tc_006a(self):
        """Identify IP metadata

        This test case consists to check that the QC Manager identifies the IP
        calibration metadata.
        """
        self.set_response_type('ip')

        self.check_responses('ordinaryControlMetric', {'level1': 'metadataRead'},
                             self.check_value_type(bool))
        Logger.info("Identifying IP metadata")

    def test_tc_006b(self):
        """Compare metadata with sensor template

        This test case consists to check that the QC Manager correctly compares
        the identified metadata with the sensor metadata template.
        """
        self.set_response_type('ip')

        self.check_responses('ordinaryControlMetric', {'level1': 'calibrationMetadata'},
                             self.check_value_type(bool))
        Logger.info("Comparing metadata with template")

    def test_tc_007a(self):
        """Reade raster layer characteristics

        This test case consists to check that the QC Manager reads the raster
        layer characteristics.
        """
        self.set_response_type('ip')

        for response in self._manager.response:
            ordinary_control_level1 = response.get(
                'ordinaryControlMetric'
            )['level1']

            assert ordinary_control_level1['channels'] > 0
            band = ordinary_control_level1['bands'][0]
            assert band['rows'] > 0
            assert band['cols'] > 0
            assert band['bits'] > 0
        Logger.info("Reading raster layer characteristics")

    def test_tc_007b(self):
        """Compare reported raster layer characteristics with real values.

        This test case consists to check that the QC Manager compares
        correctly the raster layer technical parameters with parameters
        in sensor definition file.
        """
        self.set_response_type('ip')

        from osgeo import gdal

        ref_im_path = self._manager.config['geometry']['reference_image']

        ref_im = gdal.Open(ref_im_path, gdal.GA_ReadOnly)
        # get pixel area from georeference of raster
        geo = ref_im.GetGeoTransform()
        pixel_area = abs(geo[1] * geo[5])
        # get the only band
        band = ref_im.GetRasterBand(1)

        area_ref = pixel_area * band.XSize * band.YSize
        area_band = None

        lp = self._manager.config['land_product']
        ref_im_resolution = lp['geometric_resolution']

        for response in self._manager.response:
            try:
                level2_bands = response.get(
                    'ordinaryControlMetric'
                )['level2']['bands']
            except KeyError:
                Logger.warning("IP: {} level2 not found, test007b skipped".format(
                    response.content()['properties']['identifier']
                ))

            for i in level2_bands:
                if i['id'] == 'B03_{}m'.format(ref_im_resolution):
                    rows = int(i['rows'])
                    cols = int(i['cols'])
                    res = int(i['resolution'])
                    area_band = rows * cols * res * res
                    break

        assert area_ref is not None, \
            'Band corresponding to the reference image not found'
        assert area_ref == area_band, \
            'Area of the reference image is not equal to area of ' \
            'the corresponding band'

        Logger.info("Comparing raster parameters")

    def test_tc_008(self):
        """Test if the calibration lineage is specified.

        This test case consists to check that the QC Manager identifies
        metadata lineage in the IP package.
        """
        self.set_response_type('ip')

        self.check_responses('ordinaryControlMetric', 'lineage',
                             self.check_value_type(str))
        self.check_responses('ordinaryControlMetric', {'level1': 'lineage'},
                             self.check_value_type(str))
        self.check_responses('ordinaryControlMetric', {'level2': 'lineage'},
                             self.check_value_type(str))
        Logger.info("Identifying IP lineage")

    def test_tc_009(self):
        """Identify pixel-level metadata

        This test case consists to check that the QC Manager identifies IP
        pixel-level metadata.
        """
        from processors.cloud_coverage import QCProcessorCloudCoverage

        processor_cc = QCProcessorCloudCoverage(
            self._manager.config, self._manager.response
        )
        processor_cc.run()
        assert processor_cc.get_response_status() != DbIpOperationStatus.failed

        from processors.geometry_quality import QCProcessorGeometryQuality

        processor_gq = QCProcessorGeometryQuality(
            self._manager.config, self._manager.response
        )
        processor_gq.run()
        assert processor_gq.get_response_status() != DbIpOperationStatus.failed

        self.do_009_034a()
        Logger.info("Identifying pixel-level metadata")

    def test_tc_010a(self):
        """Read detailed metadata.

        This test case consists to check that the QC Manager can read
        the detailed metadata.
        """
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'rmseX'),
                             self.check_value_type(float))
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'rmseY'),
                             self.check_value_type(float))
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'diffXmax'),
                             self.check_value_type(float))
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'diffYmax'),
                             self.check_value_type(float))
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'medianAbsShift'),
                             self.check_value_type(float))
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'validGCPs'),
                             self.check_value_type(int))

        Logger.info("Reading detailed metadata")

    def test_tc_010b(self):
        """Compare pixel metadata with specification table.

        This test case consists to check that the QC Manager compares
        correctly the pixel-level metadata with specification table.
        """
        self.check_responses('detailedControlMetric',
                             ('geometry', 0, 'requirement'),
                             self.check_value_type(bool))

        Logger.info("Comparing pixel metadata with specification table")

    def test_tc_011(self):
        """Read pixel metadata lineage

        This test case consists to check that the QC Manager can read the
        metadata lineage from image products.
        """
        self.do_011_034b()
        Logger.info("Reading pixel metadata lineage")

    def test_tc_012(self):
        """Create raster spatial layer

        This test case consists to check that the QC Manager creates spatial
        coverage layer based on selected set of quality raster metadata and map
        algebra definition.
        """
        self.do_012_036()
        Logger.info("Creating raster spatial layer")

    def test_tc_013a(self):
        """Test if the fitnessForPurpose is specified.

        This test case consists to check that the QC Manager compares
        the created spatial coverage layer with requirements defined in
        the LPST.
        """
        self.do_013a_037a()
        Logger.info("Comparing spatial coverage with specification")

    def test_tc_013b(self):
        """Return spatial coverage statistics

        This test case consists to check that the QC Manager creates spatial
        coverage comparison statistics.
        """
        self.do_013b_037b()
        Logger.info("Returning spatial coverage statistics")

    def test_tc_020(self):
        """Test if the lpInterpretationMetric appears in the metadata.

        This test case consists to check that the QC Manager identifies Land
        Product quality indicators (only UC1).
        """
        self.set_response_type('lp')
        from processors.lp_interpretation_control import QCProcessorLPInterpretationControl
        processor = QCProcessorLPInterpretationControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('lpInterpretationMetric', 'value',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Identifying Land Product QI" )

    def test_tc_021(self):
        """Compare Land Product QI with criteria

        This test case consists to check that the QC Manager compares
        the Land Product Quality Indictors with the defined criteria in LPST.
        """

        self.set_response_type('lp')

        thr = self._manager.config['land_product']['thematic_accuracy']
        self.check_responses(
            'lpInterpretationMetric',
            {'classification': 'overallAccuracy'},
            lambda value: value > thr
        )

        Logger.info("Comparing Land Product QI with criteria")

    def test_tc_022a(self):
        """Identify LP metadata

        This test case consists to check that the QC Manager identifies
        Land Product associated metadata.
        """
        self.set_response_type('lp')
        from processors.lp_metadata_control import QCProcessorLPMetadataControl
        processor = QCProcessorLPMetadataControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('lpMetadataControlMetric', 'value',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Identifying LP metadata")

    def test_tc_022b(self):
        """Compare LP metadata content with specification

        This test case consists to check that the QC Manager compares
        the Land Product associated metadata content with defined
        specification in LPST.
        """
        self.do_022b_041b()

        Logger.info("Comparing LP metadata content with specification")

    def test_tc_023a(self):
        """Read Land Product characteristics

        This test case consists to check that the QC Manager can read
        the resulting Land Product technical characteristics.
        """
        self.set_response_type('lp')
        from processors.lp_ordinary_control import QCProcessorLPOrdinaryControl
        processor = QCProcessorLPOrdinaryControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('lpOrdinaryControlMetric', 'read',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Reading Land Product characteristics")

    def test_tc_023b(self):
        """Compare LP technical characteristics with definition.

        This test case consists to check that the QC Manager compares
        the Land Product technical characteristics with the definition in
        the LPST.
        """
        self.set_response_type('lp')

        self.check_responses('lpOrdinaryControlMetric', 'value',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Comparing LP technical characteristics with definition")

    def test_tc_024a(self):
        """Read land reference data set.

        This test case consists to check that the QC Manager
        can read reference data set of the land product.
        """
        self.set_response_type('lp')
        from processors.lp_thematic_validation_control import QCProcessorLPThematicValidationControl
        processor = QCProcessorLPThematicValidationControl(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('lpThematicValidationMetric', 'value',
                             self.check_value_type(bool),
                             check_missing_value=False)

        Logger.info("Reading land reference data set")

    def test_tc_024b(self):
        """Compare LP thematic accuracy with reference.

        This test case consists to check that the QC Manager compares
        the resulting Land Product with the reference data set.
        """
        self.do_024b_042()

        Logger.info("Comparing LP thematic accuracy with reference")

    def test_tc_024c(self):
        """Create thematic validation QI.

        This test case consists to check that the QC Manager creates and
        returns the validation quality indicators.
        """
        self.set_response_type('lp')

        # use case 1: classification overall accuracy and regression RMSE
        self.check_responses('lpThematicValidationMetric',
                             {'classification': 'overallAccuracy'},
                             self.check_value_type(float),
                             check_missing_value=False)

        self.check_responses('lpThematicValidationMetric',
                             {'densityCover': 'rmse'},
                             self.check_value_type(float),
                             check_missing_value=False)

        Logger.info("Creating thematic validation QI")

    def test_tc_025(self):
        """Compare Land Product Thematic QI with criteria.

        This test case consists to check that the QC Manager compares
        the Land Product Thematic Quality Indictors with the defined criteria
        in LPST.
        """
        self.do_025_043()

        Logger.info("Checking values of the validation indicators")
