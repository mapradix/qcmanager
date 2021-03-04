import os
import datetime
import copy
from abc import ABC, abstractmethod
from requests.exceptions import ConnectionError

from processors import QCProcessorIPBase
from processors.exceptions import ProcessorFailedError

from manager.logger import Logger
from manager.logger.db import DbIpOperationStatus
from manager.io import JsonIO, CsvIO, datetime_format
from manager.catalog import QCCatalogPoller
from manager import __version__
from manager.response import QCResponse

class QCProcessorSearchBase(QCProcessorIPBase, ABC):
    """Multi-mission search processor abstract base class.
    
    :param config: processor-related config file
    :param response: processor QI metadata response managed by the manager
    """
    isMeasurementOf = "feasibilityControlMetric"

    def __init__(self, config, response):
        super(QCProcessorSearchBase, self).__init__(
            config, response
        )

    @abstractmethod
    def get_query_params(self):
        """Get query parameters."""
        pass

    @abstractmethod
    def get_product_data(self, uuid, item):
        """Get product metadata.

        :param str uuid: image UUID
        :param dict item: image metadata
        """
        pass

    def connect(self):
        """Connect provider API.

        :return obj: connector class
        """
        pt = self.get_platform_type()
        return self.connector_class(
            self.config['{}_platform'.format(pt)]['username'],
            self.config['{}_platform'.format(pt)]['password'],
            self.config['image_products']['{}_mission_archive'.format(pt)],
            self.config['image_products'].get('{}_mission_backup_archive'.format(pt))
        )

    def item2key(self, item):
        """Metadata item to query key.

        :param str item: metadata item

        :return str: query key
        """
        if item in self.conv.keys():
            return self.conv[item]

        return None

    def _get_query_params(self):
        """Get query parameters.

        :return dict: kwagrs
        """
        kwargs = {}
        for item in self.config['image_products']:
            key = self.item2key(item)
            if not key:
                continue
            kwargs[key] = self.config['image_products'][item]

        return kwargs

    def query(self, kwargs):
        """Perform search query by provider API.

        :param dict kwargs: query parameters
        
        :return dict: found IP products
        """
        if 'uuid' in self.config['image_products']:
            Logger.warning("UUID attribute given, query skipped")
            return dict.fromkeys(self.config['image_products']['uuid'], None)

        footprint = self.read_aoi(self.config['land_product']['aoi'])

        # filter by tiles (?)
        self.connector.filter_by_tiles = self.config['image_products'].get('{}_tiles'.format(
            self.get_platform_type()))

        return self.connector.query(footprint, kwargs)

    def run(self):
        """Run processor tasks.

        :return int: number of responses
        """
        self._run_start()

        metapath = os.path.join(
            self.config['project']['path'], self.config['project']['metapath']
        )

        if not os.path.isdir(metapath):
            self.create_dir(metapath)

        # connect API
        try:
            self.connector = self.connect()
        except ProcessorFailedError:
            self.set_response_status(
                DbIpOperationStatus.failed
            )
            return len(self._response)

        # filter products
        kwargs = self.get_query_params()
        products = self.query(kwargs)
        # search also for secondary product type if defined
        level2_products = None
        if self.level2_data:
            level2_producttype = self.config['image_products'].get(
                '{}_processing_level2'.format(self.platform_type.name.lower())
            )
            if level2_producttype:
                kwargs['producttype'] = level2_producttype
                Logger.debug("Query: {}".format(kwargs))
                level2_products = self.query(kwargs)

        products_count = len(products.keys())
        Logger.debug('{} products found'.format(products_count))

        # define mission archive for odata
        mission_archive = self.config['image_products']\
            ['{}_mission_archive'.format(self.platform_type.name.lower())]
        if mission_archive == 'https://scihub.copernicus.eu/dhus':
            mission_archive = 'EOP:ESA:SCIHUB:S2'
        
        # store metadata to CSV overview file and individual JSON files
        csv_data = []

        collected_files = []
        i = 0
        count = len(products)
        for uuid in products:
            i += 1
            Logger.info("Querying {} ({}/{})".format(uuid, i, count))
            # get full metadata
            if products[uuid] is None:
                continue
            odata = self.get_product_data(uuid, products[uuid])
            csv_data.append(odata)

            # compare downloaded data with already stored
            json_file = os.path.abspath(
                os.path.join(
                    metapath, '{}.geojson'.format(
                        odata[self.identifier_key])
            ))
            collected_files.append(json_file)

            # determine IP operation status
            is_new_or_updated = not os.path.exists(json_file) or self._is_updated(odata, json_file)
            if not is_new_or_updated and self.get_last_response(odata['title']) is None:
                # unchanged, but unable to get response
                is_new_or_updated = True

            if is_new_or_updated:
                # -> new IP or update IP
                # add extended metadata
                selected_for_delivery_control, response_data = self.get_response_data(
                    odata,
                    extra_data={
                        'mission_archive' : mission_archive
                    }
                )
                if selected_for_delivery_control:
                    # QA passed -> added or updated
                    if os.path.exists(json_file):
                        if self.get_last_response(odata['title']):
                            status = DbIpOperationStatus.updated
                        else:
                            # special case, unchanged, but unable to get response
                            status = DbIpOperationStatus.forced
                    else:
                        status = DbIpOperationStatus.added
                else:
                    # QA not passed -> rejected
                    status = DbIpOperationStatus.rejected
                # prepare response (must be called before saving)
                timestamp = datetime.datetime.now()
                if not os.path.exists(json_file):
                    search_date = timestamp
                else:
                    try:
                        search_date = JsonIO.read(json_file)['qcmms_search_date']
                    except KeyError:
                        # appears on updated, byt unable to get response
                        search_date = timestamp

                response_data["properties"]\
                    ["productInformation"]\
                    ["qualityInformation"]\
                    ["qualityIndicators"].append(
                    {
		        "searchDate": search_date,
		        "searchDateUpdate": timestamp,
		        "isMeasurementOf": "{}/#{}".format(
                            self._measurement_prefix, self.isMeasurementOf),
                "generatedAtTime": datetime.datetime.now(),
		        "value": selected_for_delivery_control,
                        "lineage": 'http://qcmms.esa.int/QCMMS_QCManager_v{}'.format(
                            __version__
                        )
		    }
                )

                # pair level1 products with level2 if exists
                if selected_for_delivery_control and level2_products:
                    l2_found = False
                    for uuid in level2_products:
                        try:
                            title_items_s = level2_products[uuid]['title'].split('_')
                        except TypeError:
                            continue
                        title_items_p = odata['title'].replace('L1C', 'L2A').split('_')
                        title_items_s[3] = title_items_p[3] = 'N' # version differs
                        if title_items_s == title_items_p:
                            Logger.debug("Level2 product found: {}".format(
                                level2_products[uuid]['title']
                            ))
                            odata['qcmms'] = {
                                'processing_level2': {
                                    'id' : uuid,
                                    'title' : level2_products[uuid]['title']
                                }
                            }
                            l2_found = True
                            break

                    if not l2_found:
                        Logger.info("Secondary product not found")

                # save searched products into individual geojsons
                JsonIO.write(json_file, odata)
            else:
                # -> unchanged, read last response
                response_data = self.get_last_response(odata['title'])
                selected_for_delivery = QCResponse(response_data).get_value(
                    self.isMeasurementOf
                )
                status = DbIpOperationStatus.unchanged \
                    if selected_for_delivery else DbIpOperationStatus.rejected

            # add response from file
            self.add_response(
                response_data
            )

            # log processor IP operation
            timestamp = self.file_timestamp(json_file)
            self.ip_operation(odata[self.identifier_key], status,
                              timestamp=timestamp
            )

        # check for deleted
        # processed_ids_delete = Logger.db_handler().processed_ips(
        #     self.identifier, prev=True, platform_type=self.platform_type
        # )
        # for ip, status in processed_ids_delete:
        #     if status == DbIpOperationStatus.deleted:
        #         # already deleted, skip
        #         continue
        #     json_file = os.path.abspath(
        #         os.path.join(metapath, '{}.geojson'.format(ip))
        #     )
        #     if json_file in collected_files:
        #         continue

        #     # to be removed
        #     os.remove(json_file)
        #     self.ip_operation(ip, DbIpOperationStatus.deleted)

        if len(csv_data) > 0:
            csv_file = os.path.join(
                metapath,
                '{}_fullmetadata.csv'.format(
                    self.config['land_product']['product_abbrev']
                )
            )
            CsvIO.write(csv_file, csv_data, append=True)

        if len(collected_files) < 1:
            Logger.warning("No products found")

        self._run_done()

        return len(self._response)

    def _is_updated(self, data, json_file):
        """Check if data are updated.

        :param dict data: data to be checked
        :param str: filename with alreadys stored data

        :return bool: True if updated otherwise False
        """
        updated = False
        json_data = JsonIO.read(json_file)
        is_l2a = json_data['title'].find('MSIL2A') > -1

        # check for updated items first
        for k, v in json_data.items():
            if k in data.keys() and data[k] != v:
                if isinstance(v, datetime.datetime):
                    dt = datetime.datetime.strptime(
                        data[k], '%Y-%m-%dT%H:%M:%S.%f'
                    )
                    if (dt - v).total_seconds() < 0.01:
                        # timedelta under threshold
                        continue
                Logger.info("Change in file {} detected ({}: {} -> {})".format(
                    os.path.basename(json_file), k, data[k], v
                ))
                updated = True

        # check for added/deleted items
        if len(data.keys()) != len(json_data.keys()):
            for k in data.keys():
                if k not in json_data:
                    Logger.info("Change in file {} detected ({} removed)".format(
                        os.path.basename(json_file), k
                    ))
                    updated = True
            for k in json_data.keys():
                if k == 'qcmms':
                    # ignore QCMMS metadata if any
                    continue
                if is_l2a and k in ('Tile Identifier horizontal order',
                                    'Datatake sensing start',
                                    'Instrument mode',
                                    'Tile Identifier'):
                    # ignore extra metadata items for L2A products
                    continue

                if k not in data:
                    Logger.info("Change in file {} detected ({} added)".format(
                        os.path.basename(json_file), k
                    ))
                    updated = True

        if not updated:
            Logger.debug("No changes in file {} detected".format(
                os.path.basename(json_file)
            ))

        return updated

    def get_last_response(self, identifier):
        """Get response from previous job.

        :param str identifier: IP identifier
        """
        data = super(QCProcessorSearchBase, self).get_last_response(
            identifier, full=True)

        try:
            qi = data['properties']['productInformation']\
                ['qualityInformation']['qualityIndicators']
        except TypeError:
            Logger.debug("Broken previous job. Creating new response.")
            return None

        # search for feasibilityControlMetric
        idx = 0
        for item in qi:
            if item["isMeasurementOf"].endswith('feasibilityControlMetric'):
                break
            idx += 1

        # remove deliveryControlMetric, ...
        data['properties']['productInformation']\
            ['qualityInformation']['qualityIndicators'] = qi[:idx+1]

        return data

    def get_response_data(self, data, extra_data={}):
        """Get response data.

        :param dict data: IP metadata
        :param dict extra_data: additional data to be included

        :return dict: response data
        """
        return {
            "type" : "Feature",
            "id" : "http://fedeo.esa.int/opensearch/request/?httpAccept=application/geo%2Bjson&parentIdentifier={0}&uid={1}".format(
                extra_data['mission_archive'], data['title']
            ),
            "bbox" : extra_data['bbox'],
            "geometry" : extra_data['geometry'],
            "properties" : {
                "status" : "ARCHIVED",
                "kind": "http://purl.org/dc/dcmitype/Dataset",
                "parentIdentifier" : self.get_parent_identifier(),
                "collection" : self.config['catalog']['collection'],
                "identifier" : data['title'],
                "title" : data['title'],
                "date" : "{0}/{1}".format(
                    datetime_format(data['Sensing start']),
                    datetime_format(data['Sensing stop'])
                ),
                "updated" : data['Ingestion Date'],
                "additionalAttributes": {
                    "s2datatakeid" : data['Mission datatake id'],
                    "processingbaseline" : str(data['Processing baseline']),
                    "uuid" : data['id'],
                    "platformidentifier" : data['NSSDC identifier'],
                    "hv_order_tileid" : data['Tile Identifier horizontal order'],
                    "datatakesensingstart" : data['Datatake sensing start']
                },
                "links" : {
                    "data" : [ {
                        "href" : "https://scihub.copernicus.eu/dhus/odata/v1/Products('${id}')/$value".format(**data),
                        "type" : "application/x-binary",
                        "title" : "Download"
                    } ],
                    "previews" : [ {
                        "href" : "https://scihub.copernicus.eu/dhus/odata/v1/Products('${id}')/Products('Quicklook')/$value".format(**data),
                        "type" : "image/jpeg",
                        "title" : "Quicklook"
                    } ]
                },
                "acquisitionInformation" : [ {
                    "platform" : {
                        "platformShortName" : data['Satellite name'],
                        "platformSerialIdentifier" : data['Satellite number']
                    },
                    "instrument" : {
                        "instrumentShortName" : data['Instrument'],
                        "sensorType" : "OPTICAL"
                    },
                    "acquisitionParameters" : {
                        "operationalMode" : data['Instrument mode'],
                        "beginningDateTime" : data['Sensing start'],
                        "endingDateTime" : data['Sensing stop'],
                        "orbitDirection" : data['Pass direction'],
                        "orbitNumber" : data['Orbit number (start)'],
                        "relativeOrbitNumber" : data['Relative orbit (start)'],
                        "tileId" : data['Tile Identifier'],
                        "acquisitionType" : "NOMINAL"
                    }
                } ],
                "productInformation" : {
                    "productType" : data['Product type'],
                    "availabilityTime" : data['Ingestion Date'],
                    "referenceSystemIdentifier" : "epsg:4326",
                    "cloudCover" : data['Cloud cover percentage'],
                    "processingLevel": extra_data['processingLevel'],
                    "processorVersion" : str(data['Processing baseline']),
                    "size" : extra_data['size'],
                    "format" : data['Format'],
	            "qualityInformation" : {
                        "qualityDegradation" : extra_data['qualityDegradation'],
		        "qualityIndicators": [
		            {
			        "isMeasurementOf": "{}/#degradedDataPercentageMetric".format(
                                    self._measurement_prefix),
			        "value": data['Degraded MSI data percentage']
		            },
		            {
                                "isMeasurementOf": "{}/#degradedAncillaryDataPercentageMetric".format(
                                    self._measurement_prefix),
		                "value": data['Degraded ancillary data percentage']
		            },
		            {
			        "isMeasurementOf": "{}/#formatCorrectnessMetric".format(
                                    self._measurement_prefix),
			        "value": extra_data['formatCorrectnessMetric']
		            },
		            {
			        "isMeasurementOf": "{}/#generalQualityMetric".format(
                                    self._measurement_prefix),
			        "value": extra_data['generalQualityMetric']
		            },
		            {
			        "isMeasurementOf": "{}/#geometricQualityMetric".format(
                                    self._measurement_prefix),
			        "value": extra_data['geometricQualityMetric']
		            },
		            {
			        "isMeasurementOf": "{}/#radiometricQualityMetric".format(
                                    self._measurement_prefix),
			        "value": extra_data['radiometricQualityMetric']
		            },
		            {
			        "isMeasurementOf": "{}/#sensorQualityMetric".format(
                                    self._measurement_prefix),
			        "value": extra_data['sensorQualityMetric']
		            }
                        ]
                    }
                }
            }
        }


