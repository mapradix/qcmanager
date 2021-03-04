import json
from requests.exceptions import ConnectionError
from collections import OrderedDict

from processors.search.base import QCProcessorSearchBase
from processors.exceptions import ProcessorFailedError
from processors.utils import wkt2bbox, wkt2json
from processors.sentinel import QCProcessorSentinelMeta

from manager.logger import Logger
from manager.io import datetime_format


class QCConnectSentinel:
    def __init__(self, username, password, archive, backup_archive=None):
        """Connect API.

        Raise ProcessorFailedError on failure
        """
        from sentinelsat.sentinel import SentinelAPI, SentinelAPIError

        # remember settings for query()
        self.archive = archive
        self.backup_archive = backup_archive

        # connect API
        try:
            self.api = SentinelAPI(
                username, password, archive
            )
        except (SentinelAPIError, ConnectionError) as e:
            self.api = None
            if backup_archive:
                # re-try with backup archive
                Logger.error("Unable to connect {} ({}). Re-trying with {}...".format(
                    archive, e, backup_archive
                ))
                try:
                    self.api = SentinelAPI(
                        username, password, backup_archive
                    )
                except (SentinelAPIError, ConnectionError) as e:
                    self.api = None

            if self.api is None:
                raise ProcessorFailedError(
                    self,
                    "Unable to connect: {}".format(e),
                    set_status=False
                )

        Logger.debug("Sentinel API connected")


    def query(self, footprint, kwargs):
        """Query API.

        Raise ProcessorFailedError on failure

        :return: result
        """
        from sentinelsat.sentinel import SentinelAPIError

        result = None
        try:
            result = self.api.query(footprint, **kwargs)
        except (SentinelAPIError, ConnectionError) as e:
            if self.backup_archive:
                # re-try with backup archive
                Logger.error("Unable to access {}. Re-trying with {}...".format(
                    self.archive, self.backup_archive
                ))
                self.api.api_url = self.backup_archive

                try:
                    result = self.api.query(footprint, **kwargs)
                except (SentinelAPIError, ConnectionError) as e:
                    pass # exception will be raised anyway

            raise ProcessorFailedError(
                self,
                "Unable to query API: {}".format(e),
                set_status=False
            )

        if kwargs['producttype'] == 'S2MSI1C' and self.filter_by_tiles:
            result_filtered = OrderedDict()
            for ip, items in result.items():
                for tile in self.filter_by_tiles:
                    if tile == items['tileid']:
                        result_filtered[ip] = items
                        break
                if ip not in result_filtered.keys():
                    Logger.info("IP {} skipped by tile filter".format(ip))
            return result_filtered

        return result

class QCProcessorSearchSentinel(QCProcessorSearchBase, QCProcessorSentinelMeta):
    """Sentinelsat-based search processor.
    """
    connector_class = QCConnectSentinel

    # used by item2key()
    conv = {
        'primary_platform': 'platformname',
        'primary_processing_level1': 'producttype',
        'max_cc_pct': 'cloudcoverpercentage',
        'datefrom': 'datefrom',
        'dateto': 'dateto',
    }
    identifier_key = 'title'

    def check_dependency(self):
        from sentinelsat.sentinel import SentinelAPI

    def get_query_params(self):
        """Get query.

        :return dict: query parameters
        """
        kwargs = self._get_query_params()

        if 'cloudcoverpercentage' in kwargs:
            kwargs['cloudcoverpercentage'] = (0, kwargs['cloudcoverpercentage'])
        if 'datefrom' in kwargs and 'dateto' in kwargs:
            kwargs['date'] = (kwargs['datefrom'], kwargs['dateto'])
            del kwargs['datefrom']
            del kwargs['dateto']

        Logger.debug("Query: {}".format(kwargs))
        Logger.info("config - processed")

        return kwargs

    def get_product_data(self, uuid, item):
        return self.connector.api.get_product_odata(
                uuid, full=True
        )

    def get_response_data(self, data, extra_data={}):
        # select for delivery control?
        qi_failed = []
        for attr in ('Format correctness',
                     'General quality',
                     'Geometric quality',
                     'Radiometric quality',
                     'Sensor quality'):
            if data[attr] != 'PASSED':
                qi_failed.append(attr)
        selected_for_delivery_control = len(qi_failed) < 1
        if qi_failed:
            # log reason why it's failing
            Logger.info("Rejected because of {}".format(','.join(qi_failed)))

        extra_data['bbox'] = wkt2bbox(data['footprint'])
        extra_data['geometry'] = json.loads(wkt2json(data['footprint']))
        extra_data['qualityDegradation'] = max(
            float(data['Degraded MSI data percentage']),
            float(data['Degraded ancillary data percentage'])
        )
        extra_data['processingLevel'] = data['Processing level'].split('-')[1]
        extra_data['size'] = int(float(data['Size'].split(' ')[0]) * 1000)
        extra_data['formatCorrectnessMetric'] = data['Format correctness'] == 'PASSED'
        extra_data['generalQualityMetric'] = data['General quality'] == 'PASSED'
        extra_data['geometricQualityMetric'] = data['Geometric quality'] == 'PASSED'
        extra_data['radiometricQualityMetric'] = data['Radiometric quality'] == 'PASSED'
        extra_data['sensorQualityMetric'] = data['Sensor quality'] == 'PASSED'

        return selected_for_delivery_control, \
            super(QCProcessorSearchSentinel, self).get_response_data(
                data, extra_data
        )
