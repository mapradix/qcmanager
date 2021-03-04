import json
import datetime

from processors.utils import wkt2json
from processors import QCProcessorLPBase, identifier_from_file

class QCProcessorLPInit(QCProcessorLPBase):
    """Processor performing LP initialization.
    """
    identifier = identifier_from_file(__file__)
    isMeasurementOf = "ipForLpInformationMetric"

    def check_dependency(self):
        """Check processor's software dependecies.
        """
        pass

    def get_sensors(self):
        """Get acquisition sensors information.

        :return dict: acquisition information
        """
        acquisition_information = []
        if self.config['image_products']['primary_platform'] == 'Sentinel-2':
            acquisition_information.append(
                {
                    "platform": {
                        "id": "https://earth.esa.int/concept/sentinel-2",
                        "platformShortName": "Sentinel-2"
                    },
                    "instrument": {
                        "id": "https://earth.esa.int/concept/s2-msi",
                        "instrumentShortName": "MSI"
                    }
                }
            )

        if 'supplementary_platform' in self.config['image_products'].keys() and \
                self.config['image_products']['supplementary_platform'] == 'Landsat-8':
            acquisition_information.append(
                {
                    "instrument": {
                        "instrumentShortName": "OLI",
                        "id": "https://earth.esa.int/concept/oli"
                    },
                    "platform": {
                        "platformShortName": "Landsat-8",
                        "id": "https://earth.esa.int/concept/landsat-8"
                    }
                }
            )

        return acquisition_information
    
    def get_raster_coding(self):
        """Get raster coding from configuration.

        :return list: list of raster coding
        """
        rc = []
        for value, label in self.config['land_product']['raster_coding'].items():
            if value == 'classification':            
                for k in label:
                    rc.append({
                           "name": 'classification_' + k,
                           "min": label[k],
                           "max": label[k]
                    })
            elif value == 'regression':
                rc.append({
                         "name": "regression",
                         "min": label['min'],
                         "max": label['max']
                })
            elif value == 'unclassifiable' or value == 'out_of_aoi':
                rc.append({
                        "name": value,
                        "min": label,
                        "max": label
                })

        return rc

    def run(self):
        """Run processor.
        """
        # log start computation
        self._run_start()

        self.add_response(
            self._run()
        )

        # log computation finished
        self._run_done()

    def _run(self):
        """Perform processor's tasks.

        :return dict: QI metadata
        """
        from sentinelsat.sentinel import geojson_to_wkt, read_geojson

        return {
            "type": "Feature",
            "id": "http://qcmms-cat.spacebel.be/eo-catalog/series/{}/datasets/{}".format(
                self.config['catalog']['lp_parent_identifier'],
                self.config['land_product']['product_abbrev']),
            "geometry": json.loads(wkt2json(self.read_aoi(self.config['land_product']['aoi']))),
            "properties": {
                "title": self.config['land_product']['product_abbrev'],
                "identifier": self.config['land_product']['product_abbrev'],
                "status": "PLANNED",
                "kind": "http://purl.org/dc/dcmitype/Dataset",
                "parentIdentifier": self.config['catalog']['lp_parent_identifier'],
                "abstract": self.config['land_product']['product_abstract'],
                # "date": "2020-05-12T00:00:00Z/2020-05-12T23:59:59Z"
                "date": datetime.datetime.now(),
                "categories": [
                    {
                        "term": "https://earth.esa.int/concept/" + self.config['land_product']['product_term'],
                        "label": self.config['land_product']['product_term']
                    },
                    {
                        "term": "http://www.eionet.europa.eu/gemet/concept/4599",
                        "label": "land"
                    },
                    {
                        "term": "https://earth.esa.int/concept/sentinel-2",
                        "label": "Sentinel-2"
                    }
                ],
                "updated": datetime.datetime.now(),
                "qualifiedAttribution": [
                    {
                        "type": "Attribution",
                        "agent": [
                            {
                                "type": "Organization",
                                "email": "eohelp@eo.esa.int",
                                "name": "ESA/ESRIN",
                                "phone": "tel:+39 06 94180777",
                                "uri": "http://www.earth.esa.int",
                                "hasAddress": {
                                    "country-name": "Italy",
                                    "postal-code": "00044",
                                    "locality": "Frascati",
                                "street-address": "Via Galileo Galilei CP. 64"
                                }
                            }                            
                        ],
                    "role": "originator"
                    }
                ],
                "acquisitionInformation": self.get_sensors(),
                "productInformation": {
                    "productType": "classification",
                    "availabilityTime": "2019-06-20T15:23:57Z",
                    "format": "geoTIFF",
                    "referenceSystemIdentifier": "http://www.opengis.net/def/crs/EPSG/0/3035",
                    "qualityInformation": {
                        "qualityIndicators": []
                    }
                },
                "additionalAttributes": {
                    "product_focus": self.config['land_product']['product_focus'],
                    "lpReference": "EN-EEA.IDM.R0.18.009_Annex_8 Table 11",
                    "temporal_coverage": self.config['land_product']['temporal_focus'],
                    "geometric_resolution": self.config['land_product']['geometric_resolution'],
                    "grid": self.config['land_product']['grid'],
                    "crs": self.config['land_product']['crs'],
                    "geometric_accuracy": self.config['land_product']['geometric_accuracy'],
                    "thematic_accuracy": self.config['land_product']['thematic_accuracy'],
                    "data_type": self.config['land_product']['data_type'],
                    "mmu_pixels": self.config['land_product']['mmu_pixels'],
                    "necessary_attributes": self.config['land_product']['necessary_attributes'].split(','),
                    "rasterCoding": self.get_raster_coding(),
                    "seasonal_window": self.config['image_products']['seasonal_window'],
                },
                "links": {
                    "via": [
                        {
                            "href": "http://qcmms-cat.spacebel.be/eo-catalog/series/{}/datasets".format(
                                self.config['catalog']['collection']
                            ),
                            "type": "application/geo+json",
                            "title": "Input data"
                        }
                    ]
                }
            }
        }

