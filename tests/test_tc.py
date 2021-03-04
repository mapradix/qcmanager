import os
import copy

from manager import QCManager
from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus

def _cleanup(request, config_files):
    # clean-up
    QCManager(
        config_files,
        cleanup=-1
    )# .cleanup_data()

def _setup(request, config_files):
    # run manager for tests
    request.cls._manager = QCManager(
        config_files,
        quiet=True
    )
    request.cls._num_responses = {
        'ip': 0,
        'lp': 0
    }
    request.cls._response = {
        'ip': [],
        'lp': [],
        'current': None
    }

def _teardown(request):
    for response_type in ('ip', 'lp'):
        request.cls._manager.response = copy.deepcopy(request.cls._response[response_type])
        request.cls._manager.save_response()


class TestsBase:
    """Base class for tests.

    Contains tests mutual for both TUCs and methods useful for more tests.
    """

    def set_response_type(self, response_type, copy_responses=False):
        """Set response type to IP or LP.

        :param response_type: Either 'ip' or 'lp'
        :param copy_responses: If True, it copies the old response into the
            current one, because it can be lost after a previous reset
        """
        if response_type != self._response['current']:
            self._manager.reset_response()
        if copy_responses:
            self._manager.response = copy.deepcopy(self._response[response_type])
        self._response['current'] = response_type

    def check_responses(self, isMeasurementOf, attribute, condition,
                        check_missing_value=True):
        def get_dict_value(item, attribute):
            try:
                current = item[list(attribute.keys())[0]]
                if not isinstance(current, list):
                    current = [current]
                value = []
                for c in current:
                    value.append(c.get(list(attribute.values())[0]))
            except KeyError:
                value = None

            return value

        def get_value(item, attribute):
            return [item.get(attribute)]

        # update: copy responses from manager
        response_type = self._response['current']
        self._response[response_type] = copy.deepcopy(self._manager.response)

        for response in self._response[response_type]:
            status = response.status
            item = response.get(
                isMeasurementOf
            )
            value = None
            if item:
                if isinstance(attribute, dict):
                    value = get_dict_value(item, attribute)
                elif isinstance(attribute, tuple):
                    value_list = None

                    if isinstance(attribute[0], str):
                        func = get_value
                    elif isinstance(attribute[0], dict):
                        func = get_dict_value

                    value_list = func(item, attribute[0])

                    if value_list is not None:
                        value = []
                        for v in value_list:
                            v_index = v[attribute[1]]
                            val = func(v_index, attribute[2])[0]
                            value.append(val)
                else:
                    value = get_value(item, attribute)

            if isinstance(value, list):
                non_none_vals = [v for v in value if v is not None]
                if len(non_none_vals) == 0:
                    value = None

            if value is None:
                if check_missing_value:
                    # missing value only allowed for rejected IP
                    assert response.status == DbIpOperationStatus.rejected
            else:

                for v in non_none_vals:
                    assert condition(v), 'Wrong value of attribute {}'.format(attribute)

    @staticmethod
    def check_value_one(allowed_value):
        """Check if the value is equal to the allowed one.

        :param allowed_value: Allowed value
        """
        return lambda value: value == allowed_value

    @staticmethod
    def check_value_tuple(allowed_values):
        """Check if the value is equal to the allowed one.

        :param allowed_values: Tuple of allowed values
        """
        return lambda value: value in allowed_values

    @staticmethod
    def check_value_type(allowed_type):
        """Check if the value is equal to the allowed one.

        :param allowed_type: Allowed data type of value
        """
        return lambda value: isinstance(value, allowed_type)

    def check_file_exists(self):
        return lambda value: os.path.isfile(
            os.path.join(self._manager.config['project']['path'], value)
        )

    def check_pixel_coding(self, allowed_value):
        def _check_pixel_coding(path, filename, allowed_value):
            from osgeo import gdal
            import numpy as np
            basename, ext = os.path.splitext(filename)
            if ext == '.gml':
                # skip vector mask file
                return True
            else:
                ext = '.tif' # switch from JPEG (RGB) to TIF (one band)
            ds = gdal.Open(os.path.join(path, basename + ext))
            band = ds.GetRasterBand(1)
            array = np.array(band.ReadAsArray())
            ds = None
            value = np.unique(array, return_counts=False)
            value.sort()

            return all([i in allowed_value for i in value])

        return lambda value: _check_pixel_coding(
            self._manager.config['project']['path'],
            value,
            allowed_value
        )

    def get_platform(self, ptype):
        try:
            return self._manager.config['image_products']['{}_platform'.format(ptype)]
        except KeyError:
            # platform not defined in config
            pass

        return None

    def do_001_030(self):
        """Reading LPST parameters

        This test case consists to check that the QC Manager reads the LPST
        correctly.
        """
        self.set_response_type('ip')

        assert self._manager.config.has_section('land_product')
        assert self._manager.config['land_product']['product_abbrev']
        assert self._manager.config['land_product']['geometric_resolution'] > 0
        assert self._manager.config['land_product']['epsg'] > 0
        assert self._manager.config['land_product']['geometric_accuracy'] > 0
        assert self._manager.config['land_product']['thematic_accuracy'] > 0

    def do_002a_031(self):
        """Creating metadata request

        This test case consists to check that the QC Manager creates the
        metadata request and send it to the third party providers interface
        (e.g. sentinelsat).
        """
        self.set_response_type('ip')

        from processors.search import QCProcessorSearch

        for ptype in ('primary', 'supplementary'):
            platform = self.get_platform(ptype)
            if not platform:
                continue
            processor = QCProcessorSearch(self._manager.config, self._manager.response).\
                get_processor_sensor(platform, ptype)
            kwargs = processor.get_query_params()

            assert kwargs['producttype'] == self._manager.config['image_products']\
                ['{}_processing_level1'.format(ptype)]

    def do_003_032(self):
        """Selecting quality IP

        This test case consists to check that the QC Manager selects quality IP
        based on criteria filter applied on the metadata.
        """
        self.set_response_type('ip')

        from processors.search import QCProcessorSearch

        processor = QCProcessorSearch(
            self._manager.config, self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('feasibilityControlMetric', 'value',
                             self.check_value_type(bool))

    def do_004a_033a(self):
        """Identifying delivered IP

        This test case consists to check that the QC Manager identifies all
        delivered (locally downloaded) image products for further processing.
        """
        self.set_response_type('ip')

        from processors.download import QCProcessorDownload

        processor = QCProcessorDownload(
            self._manager.config, self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        # collect files
        dir_content = os.listdir(os.path.join(
            self._manager.config['project']['path'],
            self._manager.config['project']['downpath'])
        )
        for response in self._manager.response:
            identifier = response.content()['properties']['identifier']
            assert len(list(filter(lambda x: x.startswith(identifier), dir_content))) > 0

    def do_004b_033a(self):
        """Comparing delivery IP with expected

        This test case consists to check that the downloaded IP validated the MD5 checksum.
        """
        self.set_response_type('ip')

        self.check_responses('deliveryControlMetric', 'value',
                             self.check_value_type(bool))

    def do_009_034a(self):
        """Identifying pixel-level metadata

        This test case consists to check that the QC Manager identifies IP
        pixel-level metadata.
        """
        self.set_response_type('ip')

        from processors.valid_pixels import QCProcessorValidPixels
        processor_vp = QCProcessorValidPixels(
            self._manager.config,
            self._manager.response
        )
        processor_vp.run()
        assert processor_vp.get_response_status() != DbIpOperationStatus.failed

        self.check_responses('detailedControlMetric', {'cloudCover' : 'mask'},
                             self.check_file_exists())
        self.check_responses('detailedControlMetric', {'validPixels' : 'mask'},
                             self.check_file_exists())

    def do_011_034b(self):
        """Reading pixel metadata lineage

        This test case consists to check that the QC Manager can read the
        metadata lineage from image products.
        """
        self.set_response_type('ip')

        self.check_responses('detailedControlMetric', {'cloudCover' : 'lineage'},
                             self.check_value_type(str))
        self.check_responses('detailedControlMetric', {'validPixels' : 'lineage'},
                             self.check_value_type(str))

    def do_012_036(self):
        """Creating raster spatial layer

        This test case consists to check that the QC Manager creates spatial
        coverage layer based on selected set of quality raster metadata and map
        algebra definition.
        """
        self.set_response_type('lp')

        # run LP initialization before vpx coverage
        from processors.lp_init import QCProcessorLPInit
        processor = QCProcessorLPInit(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        # compute vpx coverage
        from processors.vpx_coverage import QCProcessorVpxCoverage
        processor = QCProcessorVpxCoverage(
            self._manager.config,
            self._manager.response
        )
        processor.run()
        assert processor.get_response_status() != DbIpOperationStatus.failed

        self._num_responses['lp'] += len(self._manager.response)

        for year in processor.get_years():
            assert os.path.exists(processor.get_output_file(year))

    def do_013a_037a(self):
        """Test if the fitnessForPurpose is specified.

        This test case consists to check that the QC Manager compares
        the created spatial coverage layer with requirements defined in
        the LPST.
        """
        self.check_responses('ipForLpInformationMetric', 'fitnessForPurpose',
                             self.check_value_type(str))

    def do_013b_037b(self):
        """Returning spatial coverage statistics

        This test case consists to check that the QC Manager creates spatial
        coverage comparison statistics.
        """
        from processors.vpx_coverage import QCProcessorVpxCoverage
        processor = QCProcessorVpxCoverage(
            self._manager.config,
            self._manager.response
        )
        # no need to run processor, already performed by 012 (!)

        for year in processor.get_years():
            value, count, ncells = processor.compute_value_count(
                processor.get_output_file(year)
            )
            assert max(value) <= len(self._response['ip'])

    def do_022b_041b(self):
        """Compare LP metadata content with specification

        This test case consists to check that the QC Manager compares
        the Land Product associated metadata content with defined
        specification in LPST.
        """
        self.set_response_type('lp')

        self.check_responses('lpMetadataControlMetric', 'metadataCompliancy',
                             self.check_value_type(bool),
                             check_missing_value = False)

    def do_024b_042(self):
        """Compare LP thematic accuracy with reference.

        This test case consists to check that the QC Manager compares
        the resulting Land Product with the reference data set.
        """
        self.set_response_type('lp')

        # thr = self._manager.config['land_product']['thematic_accuracy']
        self.check_responses(
            'lpThematicValidationMetric',
            {'classification': 'overallAccuracy'},
            lambda value: value > 0.0
        )

    def do_025_043(self):
        """Compare Land Product Thematic QI with criteria.

        This test case consists to check that the QC Manager compares
        the Land Product Thematic Quality Indictors with the defined criteria
        in LPST.
        """
        self.set_response_type('lp')

        self.check_responses('lpThematicValidationMetric', 'value',
                             self.check_value_type(bool),
                             check_missing_value=False)

