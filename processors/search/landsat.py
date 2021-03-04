import json
from datetime import datetime

from processors.search.base import QCProcessorSearchBase
from processors.exceptions import ProcessorFailedError
from processors.utils import wkt2bbox
from processors.landsat import QCProcessorLandsatMeta

from manager.logger import Logger


class QCConnectLandsat:
    def __init__(self, username, password, archive, backup_archive):
        """Connect API.

        Raise ProcessorFailedError on failure

        :param str username: username
        :param str password: password
        :param str archive: not used by Landsat implementation
        :param str backup_archive: not used by Landsat implementation
        """
        from landsatxplore.api import API as LandsatAPI
        from landsatxplore.exceptions import EarthExplorerError

        try:
            self.api = LandsatAPI(
                username, password
            )
        except EarthExplorerError as e:
            raise ProcessorFailedError(
                self,
                "Unable to connect API: {}".format(e),
                set_status=False
            )
        except json.JSONDecodeError as e:
            raise ProcessorFailedError(
                self,
                "Landsat server is down. It raised a JSON exception: "
                "{}".format(e),
                set_status=False
            )

    def __del__(self):
        if not hasattr(self, "api"):
            return

        from landsatxplore.exceptions import EarthExplorerError
        try:
            self.api.logout()
        except EarthExplorerError as e:
            Logger.error("Landsat server is down. {}".format(e))

    def query(self, footprint, kwargs):
        """Query API.

        :return: result
        """
        from landsatxplore.exceptions import EarthExplorerError

        kwargs['bbox'] = wkt2bbox(footprint, switch_axis=True)
        kwargs['max_results'] = 500
        del kwargs['producttype'] # used only for testing
        try:
            items = self.api.search(**kwargs)
        except EarthExplorerError as e:
            raise ProcessorFailedError(
                self,
                "Landsat server is down. "
                "{}".format(e),
                set_status=False
            )

        dict_items = {}
        for item in items:
            selected = False
            if self.filter_by_tiles:
                for tile in self.filter_by_tiles:
                    if str(tile) in item['entityId']:
                        selected = True
                        break
            else:
                selected = True
            if selected:
                dict_items[item['entityId']] = item
                # used tests only
                dict_items[item['entityId']]['producttype'] = item['displayId'].split('_')[1]
                dict_items[item['entityId']]['beginposition'] = \
                    datetime.strptime(item['startTime'], '%Y-%m-%d')
            else:
                Logger.info("IP {} skipped by tile filter".format(item['entityId']))

        return dict_items


class QCProcessorSearchLandsat(QCProcessorSearchBase, QCProcessorLandsatMeta):
    """Landsat8-based search processor.
    """
    connector_class = QCConnectLandsat
    level2_data = False

    # used by item2key()
    conv = {
        'supplementary_sensor': 'dataset',
        'max_cc_pct': 'max_cloud_cover',
        'datefrom': 'start_date',
        'dateto': 'end_date',
    }
    identifier_key = 'title'

    def check_dependency(self):
        from landsatxplore.api import API as LandsatAPI

    def get_query_params(self):
        kwargs = self._get_query_params()

        # date must be converted to string
        for item in ['start_date', 'end_date']:
            kwargs[item] = kwargs[item].strftime("%Y-%m-%d")
        kwargs['producttype'] = 'L1TP' # used for testing

        Logger.debug("Query: {}".format(kwargs))

        return kwargs

    def get_product_data(self, uuid, item):
        import urllib
        from xml.dom import minidom

        url = item['metadataUrl']
        response = urllib.request.urlopen(url)
        html = response.read()
        xml_meta = html.decode("utf-8")

        xmldoc = minidom.parseString(xml_meta)
        itemlist = xmldoc.getElementsByTagName('eemetadata:metadataField')
        # read metadata
        self._landsat_metadata = {}
        for s in itemlist:
            fname = s.getAttribute("name")
            metavalue = s.getElementsByTagName("eemetadata:metadataValue")[0]
            try:
                self._landsat_metadata[fname] = metavalue.firstChild.data
            except AttributeError:
                self._landsat_metadata[fname] = None

        # convert L8 metadata fields to QCMMS metadata
        odata = {}
        odata['title'] = item['displayId']
        odata['qcmms_data_href'] = item['downloadUrl']
        odata['qcmms_previews_href'] = item['browseUrl']

        odata['spatialFootprint'] = item['spatialFootprint']
        odata['sceneBounds'] = item['sceneBounds']

        # properties
        odata['use_case'] = self.get_parent_identifier()
        odata['Sensing start'] = datetime.strptime(
            self._landsat_metadata['Start Time'].split('.')[0], '%Y:%j:%H:%M:%S'
        )
        odata['Sensing stop'] = datetime.strptime(
            self._landsat_metadata['Stop Time'].split('.')[0], '%Y:%j:%H:%M:%S'
        )
        odata['Ingestion Date'] = datetime.strptime(
            self._landsat_metadata['Date L-1 Generated'], "%Y/%m/%d"
        )

        # additional attributes
        odata['Mission datatake id'] = item['entityId']
        odata['id'] = item['entityId']
        odata['NSSDC identifier'] = ''
        odata['Tile Identifier horizontal order'] = \
            (self._landsat_metadata['WRS Path']).strip() + \
            (self._landsat_metadata['WRS Row']).strip()
        odata['Datatake sensing start'] = datetime.strptime(
            item['acquisitionDate'], '%Y-%m-%d'
        )
        # July 2020 - a change in the USGS metadata -> the typo with the
        # initial space in the attribute key fixed
        if ' Processing Software Version' in self._landsat_metadata.keys():
            key = ' Processing Software Version'
            odata['Processing baseline'] = self._landsat_metadata[key]
        elif 'Processing Software Version' in self._landsat_metadata.keys():
            key = 'Processing Software Version'
            odata['Processing baseline'] = self._landsat_metadata[key]

        # acquisition information
        odata['Satellite name'] = self.config['image_products']['supplementary_platform']
        odata['Satellite number'] = ''
        odata['Instrument'] = self._landsat_metadata['Sensor Identifier']

        # acquisition parameters
        odata['Instrument mode'] = 'operational' # any other for Landsat?
        # https://landsat.usgs.gov/landsat_acq
        odata['Pass direction'] = 'DESCENDING'
        odata['Orbit number (start)'] = int((self._landsat_metadata['WRS Path']).strip())
        odata['Relative orbit (start)'] = int((self._landsat_metadata['WRS Path']).strip())
        odata['Tile Identifier'] = \
            (self._landsat_metadata['WRS Path']).strip() + \
            (self._landsat_metadata['WRS Row']).strip()

        # product information
        odata['Product type'] = self.config['image_products']['supplementary_sensor']
        # "referenceSystemIdentifier" : "epsg:4326" Datum, Ellipsoid
        odata['Cloud cover percentage'] = float(self._landsat_metadata['Scene Cloud Cover'])
        odata['Format'] = 'TIF'

        # missing QI parameters
        odata['Degraded MSI data percentage'] = 0
        odata['Degraded ancillary data percentage'] = 0

        return odata

    def get_response_data(self, data, extra_data={}):
        # number of bad scans
        l8_image_quality = {
            9: 0,
            8: 4,
            7: 4,
            6: 16,
            5: 16,
            4: 64,
            3: 64,
            2: 128,
            1: 128,
            0: 256, # (more than 33% of scene is bad)
            -1: 'NA'
        }
        bad_scans = l8_image_quality[int(self._landsat_metadata['Image Quality'])]
        if bad_scans > 0:
            extra_data['qualityDegradation'] = \
                (bad_scans / int(self._landsat_metadata['Reflective Lines'])) * 100
        else:
            extra_data['qualityDegradation'] = -1.0

        extra_data['geometry'] = json.loads(
            '{{ "type": "Polygon", "coordinates": {0} }}'.format(
                data['spatialFootprint']['coordinates']
        ))
        extra_data['bbox'] = [float(x) for x in data['sceneBounds'].split(',')]

        extra_data['processingLevel'] = \
            self._landsat_metadata['Data Type Level-1'].split('_')[-1]
        extra_data['size'] = -1
        
        extra_data['formatCorrectnessMetric'] = True
        extra_data['generalQualityMetric'] = True
        # quality information
        if self._landsat_metadata['Geometric RMSE Model X'] and \
           self._landsat_metadata['Geometric RMSE Model Y']:
            geometric_RMSE_X = float(self._landsat_metadata['Geometric RMSE Model X'])
            geometric_RMSE_Y = float(self._landsat_metadata['Geometric RMSE Model Y'])
            geometric_resolution = self.config['land_product']['geometric_resolution']
            geometric_accuracy = self.config['land_product']['geometric_accuracy']
            geometric_thr = geometric_resolution * geometric_accuracy
            if (geometric_RMSE_X <= geometric_thr) and (geometric_RMSE_Y <= geometric_thr):
                extra_data['geometricQualityMetric'] = True
            else:
                extra_data['geometricQualityMetric'] = False
        else:
            extra_data['geometricQualityMetric'] = True
        extra_data['radiometricQualityMetric'] = True
        extra_data['sensorQualityMetric'] = True

        # TBD: filter quality IP
        if extra_data['geometricQualityMetric'] is not False and \
           extra_data['qualityDegradation'] < 0.1:
            selected_for_delivery_control = True
        else:
            Logger.info(
                "Rejected: qualityDegradation={} | geometricQualityMetric={}".format(
                    extra_data['qualityDegradation'], extra_data['geometricQualityMetric']
            ))
            selected_for_delivery_control = False

        return selected_for_delivery_control, \
            super(QCProcessorSearchLandsat, self).get_response_data(
                data, extra_data
        )
